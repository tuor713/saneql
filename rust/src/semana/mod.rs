//! Semantic analysis: AST → algebra tree.
//!
//! This is a direct port of SemanticAnalysis.cpp.  The most noteworthy
//! unsafe usage is `parent_scope: Option<NonNull<BindingInfo>>`, which
//! mirrors C++'s raw `const BindingInfo*`.
//!
//! SAFETY invariant: every `NonNull<BindingInfo>` stored here points to a
//! BindingInfo that is either the static `root_scope()` or lives on the
//! call-stack of the analysis function that created it, and is therefore
//! guaranteed to outlive any use of the pointer.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::algebra::{
    AggOp, Aggregation, BinOp, CallType, CmpMode, Collate, DatePart, Expr, JoinType, MapEntry, Op,
    SetOp, SortEntry, UnOp, IU,
};
use crate::infra::schema::{Schema, SchemaProvider, Type, TypeBase};
use crate::parser::ast::{Ast, BinaryOp, FuncArg, FuncArgNamed, Literal, UnaryOp};

// ── Collate / OrderingInfo ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderingInfo {
    pub descending: bool,
    pub collate: Collate,
}

impl OrderingInfo {
    pub fn default_order() -> Self {
        Self::default()
    }
    pub fn mark_ascending(&mut self) {
        self.descending = false;
    }
    pub fn mark_descending(&mut self) {
        self.descending = true;
    }
}

// ── BindingInfo ───────────────────────────────────────────────────────────────

/// The "ambiguous" sentinel: u64::MAX used as the IU id for ambiguous columns.
#[allow(dead_code)]
const fn ambiguous_iu_id() -> u64 {
    u64::MAX
}

#[derive(Debug, Clone)]
pub struct ColumnEntry {
    pub name: String,
    pub iu: Rc<IU>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    /// None = ambiguous; Some(iu) = unique
    columns: HashMap<String, Option<Rc<IU>>>,
    ambiguous: bool,
}

#[derive(Debug, Clone, Default)]
struct Alias {
    columns: Vec<Rc<IU>>,
    ambiguous: bool,
}

/// Argument information stored in a BindingInfo's argument map.
#[derive(Clone)]
pub enum ArgInfo {
    /// A value argument: the AST node and the scope in which to evaluate it.
    ///
    /// SAFETY: if `scope` is Some, the pointer is valid for the duration of
    /// the analysis call that registered it.
    Value {
        node: Box<Ast>,
        scope: Option<NonNull<BindingInfo>>,
    },
    Symbol(String),
}

/// Group-by / window aggregate state, shared via Rc<RefCell<...>>.
pub struct GroupByState {
    pub pre_binding: BindingInfo,
    pub aggregations: Vec<Aggregation>,
    pub is_window: bool,
}

/// Tracks which columns are visible and how to look them up.
pub struct BindingInfo {
    pub columns: Vec<ColumnEntry>,
    pub column_lookup: HashMap<String, Option<Rc<IU>>>, // None = ambiguous
    scopes: HashMap<String, Scope>,
    aliases: HashMap<String, Alias>,
    arguments: HashMap<String, ArgInfo>,
    pub gbs: Option<Rc<RefCell<GroupByState>>>,
    // SAFETY: see module-level invariant.
    parent_scope: Option<NonNull<BindingInfo>>,
}

// ── BindingInfo: manual Clone that preserves the raw parent pointer ───────────

impl Clone for BindingInfo {
    fn clone(&self) -> Self {
        BindingInfo {
            columns: self.columns.clone(),
            column_lookup: self.column_lookup.clone(),
            scopes: self.scopes.clone(),
            aliases: self.aliases.clone(),
            arguments: self.arguments.clone(),
            gbs: None,                       // never clone gbs
            parent_scope: self.parent_scope, // copy pointer
        }
    }
}

// ── BindingInfo: static root ──────────────────────────────────────────────────

thread_local! {
    static ROOT_BINDING: BindingInfo = BindingInfo::new_empty();
}

impl BindingInfo {
    fn new_empty() -> Self {
        BindingInfo {
            columns: Vec::new(),
            column_lookup: HashMap::new(),
            scopes: HashMap::new(),
            aliases: HashMap::new(),
            arguments: HashMap::new(),
            gbs: None,
            parent_scope: None,
        }
    }

    pub fn new() -> Self {
        Self::new_empty()
    }

    /// Run `f` with a reference to the root (empty) binding scope.
    pub fn with_root_scope<R>(f: impl FnOnce(&BindingInfo) -> R) -> R {
        ROOT_BINDING.with(f)
    }

    /// SAFETY: `parent` must outlive `self` and must not be moved afterwards.
    pub unsafe fn set_parent(&mut self, parent: &BindingInfo) {
        self.parent_scope = Some(NonNull::from(parent));
    }

    /// Walk to the parent scope.
    /// SAFETY: caller must ensure the pointer is still valid (module invariant).
    unsafe fn parent(&self) -> Option<&BindingInfo> {
        self.parent_scope.map(|p| p.as_ref())
    }

    // ── scope management ─────────────────────────────────────────────────

    /// Add a named scope. Returns a raw pointer to the scope entry, or None if
    /// the scope already existed (→ it becomes ambiguous).
    pub(crate) fn add_scope(&mut self, name: &str) -> *mut Scope {
        if let Some(s) = self.scopes.get_mut(name) {
            s.columns.clear();
            s.ambiguous = true;
            return std::ptr::null_mut();
        }
        self.scopes.insert(name.to_string(), Scope::default());
        self.scopes.get_mut(name).unwrap() as *mut Scope
    }

    /// Add a column binding.
    /// `scope_ptr` may be null (returned from `add_scope` when scope is ambiguous).
    pub(crate) fn add_binding(&mut self, scope_ptr: *mut Scope, col: String, iu: Rc<IU>) {
        if !scope_ptr.is_null() {
            let scope = unsafe { &mut *scope_ptr };
            let e = scope.columns.entry(col.clone()).or_insert(None);
            if e.is_some() {
                *e = None;
            }
            // ambiguous
            else {
                *e = Some(Rc::clone(&iu));
            }
        }
        let e = self.column_lookup.entry(col.clone()).or_insert(None);
        if e.is_some() {
            *e = None;
        }
        // ambiguous
        else {
            *e = Some(Rc::clone(&iu));
        }
        self.columns.push(ColumnEntry { name: col, iu });
    }

    // ── lookup ────────────────────────────────────────────────────────────

    /// Unambiguous → `Ok(Some(iu))`. Not found → `Ok(None)`. Ambiguous → `Err(())`.
    pub fn lookup(&self, name: &str) -> Result<Option<Rc<IU>>, ()> {
        match self.column_lookup.get(name) {
            None => Ok(None),
            Some(None) => Err(()), // ambiguous
            Some(Some(u)) => Ok(Some(Rc::clone(u))),
        }
    }

    /// Lookup within a named scope. Returns:
    ///   Ok(Some) = found, Ok(None) = scope/column not found, Err(false) =
    ///   column ambiguous, Err(true) = scope itself ambiguous.
    pub fn lookup_scoped(&self, scope: &str, col: &str) -> Result<Option<Rc<IU>>, bool> {
        match self.scopes.get(scope) {
            None => Ok(None),
            Some(s) => {
                if s.ambiguous {
                    return Err(true);
                }
                match s.columns.get(col) {
                    None => Ok(None),
                    Some(None) => Err(false), // column ambiguous
                    Some(Some(u)) => Ok(Some(Rc::clone(u))),
                }
            }
        }
    }

    // ── arguments ─────────────────────────────────────────────────────────

    pub fn register_arg(
        &mut self,
        name: String,
        node: Box<Ast>,
        scope: Option<*const BindingInfo>,
    ) {
        let scope = scope.and_then(NonNull::new_from_ptr);
        self.arguments.insert(name, ArgInfo::Value { node, scope });
    }

    pub fn register_symbol_arg(&mut self, name: String, sym: String) {
        self.arguments.insert(name, ArgInfo::Symbol(sym));
    }

    pub fn lookup_arg(&self, name: &str) -> Option<&ArgInfo> {
        self.arguments.get(name)
    }

    // ── join / merge ──────────────────────────────────────────────────────

    pub fn join(&mut self, other: &BindingInfo) {
        self.columns.extend_from_slice(&other.columns);
        for (k, v) in &other.column_lookup {
            let e = self
                .column_lookup
                .entry(k.clone())
                .or_insert_with(|| v.clone());
            if e.is_some() && e != v {
                *e = None;
            } // ambiguous
        }
        for (k, v) in &other.scopes {
            if self.scopes.contains_key(k) {
                let s = self.scopes.get_mut(k).unwrap();
                s.columns.clear();
                s.ambiguous = true;
            } else {
                self.scopes.insert(k.clone(), v.clone());
            }
        }
        for (k, v) in &other.aliases {
            if self.aliases.contains_key(k) {
                let a = self.aliases.get_mut(k).unwrap();
                a.columns.clear();
                a.ambiguous = true;
            } else {
                self.aliases.insert(k.clone(), v.clone());
            }
        }
    }
}

// small helper so NonNull::new_from_ptr doesn't need to be repeated
trait NonNullExt<T> {
    fn new_from_ptr(p: *const T) -> Option<NonNull<T>>;
}
impl<T> NonNullExt<T> for NonNull<T> {
    fn new_from_ptr(p: *const T) -> Option<NonNull<T>> {
        NonNull::new(p as *mut T)
    }
}

// ── ExpressionResult ──────────────────────────────────────────────────────────

pub enum ExpressionResult {
    Scalar {
        expr: Box<Expr>,
        ordering: OrderingInfo,
    },
    Table {
        op: Box<Op>,
        binding: BindingInfo,
    },
}

impl ExpressionResult {
    pub fn scalar(expr: Box<Expr>) -> Self {
        ExpressionResult::Scalar {
            expr,
            ordering: OrderingInfo::default_order(),
        }
    }
    pub fn scalar_ord(expr: Box<Expr>, ordering: OrderingInfo) -> Self {
        ExpressionResult::Scalar { expr, ordering }
    }
    pub fn table(op: Box<Op>, binding: BindingInfo) -> Self {
        ExpressionResult::Table { op, binding }
    }

    pub fn is_scalar(&self) -> bool {
        matches!(self, ExpressionResult::Scalar { .. })
    }
    pub fn is_table(&self) -> bool {
        matches!(self, ExpressionResult::Table { .. })
    }

    pub fn expr_mut(&mut self) -> &mut Box<Expr> {
        match self {
            ExpressionResult::Scalar { expr, .. } => expr,
            _ => panic!("not scalar"),
        }
    }
    pub fn expr(self) -> Box<Expr> {
        match self {
            ExpressionResult::Scalar { expr, .. } => expr,
            _ => panic!("not scalar"),
        }
    }
    pub fn ordering(&self) -> OrderingInfo {
        match self {
            ExpressionResult::Scalar { ordering, .. } => *ordering,
            _ => panic!("not scalar"),
        }
    }
    pub fn ordering_mut(&mut self) -> &mut OrderingInfo {
        match self {
            ExpressionResult::Scalar { ordering, .. } => ordering,
            _ => panic!("not scalar"),
        }
    }
    pub fn op(self) -> Box<Op> {
        match self {
            ExpressionResult::Table { op, .. } => op,
            _ => panic!("not table"),
        }
    }
    pub fn binding(&self) -> &BindingInfo {
        match self {
            ExpressionResult::Table { binding, .. } => binding,
            _ => panic!("not table"),
        }
    }
    pub fn binding_mut(&mut self) -> &mut BindingInfo {
        match self {
            ExpressionResult::Table { binding, .. } => binding,
            _ => panic!("not table"),
        }
    }
    pub fn into_parts(self) -> (Box<Op>, BindingInfo) {
        match self {
            ExpressionResult::Table { op, binding } => (op, binding),
            _ => panic!("not table"),
        }
    }
}

// ── LetInfo / Signature ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCategory {
    Scalar,
    Table,
    Expression,
    ExpressionList,
    Symbol,
    SymbolList,
}

#[derive(Debug, Clone)]
pub struct SigArg {
    pub name: String,
    pub category: TypeCategory,
    pub has_default: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Signature {
    pub args: Vec<SigArg>,
}

struct LetInfo {
    sig: Signature,
    default_values: Vec<Option<Box<Ast>>>,
    body: Box<Ast>,
}

// ── SemanticAnalysis ──────────────────────────────────────────────────────────

pub struct SemanticAnalysis {
    schema: Box<dyn SchemaProvider>,
    lets: Vec<LetInfo>,
    let_lookup: HashMap<String, usize>,
    let_scope_limit: usize,
    next_symbol_id: u64,
    next_iu_id: u64,
}

impl SemanticAnalysis {
    pub fn new(schema: Schema) -> Self {
        Self::with_provider(Box::new(schema))
    }

    pub fn with_provider(schema: Box<dyn SchemaProvider>) -> Self {
        SemanticAnalysis {
            schema,
            lets: Vec::new(),
            let_lookup: HashMap::new(),
            let_scope_limit: usize::MAX,
            next_symbol_id: 1,
            next_iu_id: 1,
        }
    }

    fn new_iu(&mut self, typ: Type) -> Rc<IU> {
        let id = self.next_iu_id;
        self.next_iu_id += 1;
        IU::new(id, typ)
    }

    /// Build a table-scan result for a (possibly qualified) table name.
    ///
    /// `parts` are the individual name components, e.g.
    /// `["kafka", "default", "my.topic.name"]`.  They are joined with `.`
    /// to form the schema-lookup key passed to the [`SchemaProvider`] callback.
    fn make_table_scan(
        &mut self,
        scope: &BindingInfo,
        parts: Vec<String>,
    ) -> Result<ExpressionResult, String> {
        let lookup_key = parts.join(".");
        let table = self
            .schema
            .lookup_table(&lookup_key)
            .ok_or_else(|| format!("unknown table '{lookup_key}'"))?;
        let table_cols: Vec<_> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.typ))
            .collect();

        let mut binding = BindingInfo::new();
        unsafe {
            binding.set_parent(scope);
        }
        let scope_ptr = binding.add_scope(&lookup_key);
        let mut columns = Vec::new();
        for (col_name, typ) in table_cols {
            let iu = self.new_iu(typ);
            binding.add_binding(scope_ptr, col_name.clone(), Rc::clone(&iu));
            columns.push((col_name, iu));
        }
        Ok(ExpressionResult::table(
            Box::new(Op::TableScan { parts, columns }),
            binding,
        ))
    }

    #[allow(dead_code)]
    fn err(msg: impl Into<String>) -> String {
        msg.into()
    }

    // ── top-level entry ───────────────────────────────────────────────────

    pub fn analyze_query(&mut self, ast: &Ast) -> Result<ExpressionResult, String> {
        let qb = match ast {
            Ast::QueryBody { lets, body } => (lets, body),
            Ast::DefineFunction { .. } => return Err("defun not implemented yet".into()),
            _ => return Err("invalid top-level node".into()),
        };
        for le in qb.0 {
            self.analyze_let(le)?;
        }
        let body = qb.1.clone();
        BindingInfo::with_root_scope(|root| self.analyze_expression(root, &body))
    }

    // ── let-binding registration ──────────────────────────────────────────

    fn analyze_let(&mut self, le: &crate::parser::ast::LetEntry) -> Result<(), String> {
        let mut args: Vec<SigArg> = Vec::new();
        let mut default_values: Vec<Option<Box<Ast>>> = Vec::new();
        for a in &le.args {
            let category = match &a.typ {
                None => TypeCategory::Scalar,
                Some(t) => {
                    let name = match t {
                        crate::parser::ast::Type::Simple(n) => n.as_str(),
                        _ => return Err("complex argument types not implemented yet".into()),
                    };
                    match name {
                        "table" => TypeCategory::Table,
                        "expression" => TypeCategory::Expression,
                        "symbol" => TypeCategory::Symbol,
                        other => return Err(format!("unsupported argument type '{other}'")),
                    }
                }
            };
            let has_default = a.default.is_some();
            args.push(SigArg {
                name: a.name.clone(),
                category,
                has_default,
            });
            default_values.push(a.default.as_ref().map(|d| d.clone()));
        }
        if self.let_lookup.contains_key(&le.name) {
            return Err(format!("duplicate let '{}'", le.name));
        }
        let idx = self.lets.len();
        self.lets.push(LetInfo {
            sig: Signature { args },
            default_values,
            body: le.body.clone(),
        });
        self.let_lookup.insert(le.name.clone(), idx);
        Ok(())
    }

    // ── expression dispatch ───────────────────────────────────────────────

    fn analyze_expression(
        &mut self,
        scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<ExpressionResult, String> {
        match ast {
            Ast::Identifier(_) => self.analyze_token(scope, ast),
            Ast::Literal(lit) => self.analyze_literal(lit),
            Ast::Cast { value, typ } => self.analyze_cast(scope, value, typ),
            Ast::BinaryExpr { op, left, right } => self.analyze_binary(scope, op, left, right),
            Ast::UnaryExpr { op, value } => self.analyze_unary(scope, op, value),
            Ast::Access { base, part } => self.analyze_access(scope, base, part),
            Ast::Call { func, args } => self.analyze_call(scope, func, args),
            Ast::QueryBody { .. } => Err("unexpected QueryBody in expression position".into()),
            Ast::DefineFunction { .. } => Err("unexpected DefineFunction".into()),
        }
    }

    // ── literals ──────────────────────────────────────────────────────────

    fn analyze_literal(&mut self, lit: &Literal) -> Result<ExpressionResult, String> {
        let expr = match lit {
            Literal::Integer(s) => Expr::Const {
                value: Some(s.clone()),
                typ: Type::integer(),
            },
            Literal::Float(s) => Expr::Const {
                value: Some(s.clone()),
                typ: infer_decimal_type(s)?,
            },
            Literal::String(s) => Expr::Const {
                value: Some(s.clone()),
                typ: Type::text(),
            },
            Literal::True => Expr::Const {
                value: Some("true".into()),
                typ: Type::bool_(),
            },
            Literal::False => Expr::Const {
                value: Some("false".into()),
                typ: Type::bool_(),
            },
            Literal::Null => Expr::Const {
                value: None,
                typ: Type::unknown().as_nullable(),
            },
        };
        Ok(ExpressionResult::scalar(Box::new(expr)))
    }

    // ── identifier / table-scan / let-call without args ───────────────────

    fn analyze_token(
        &mut self,
        scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<ExpressionResult, String> {
        let name = self.extract_symbol(scope, ast)?;

        // Column reference?
        match scope.lookup(&name) {
            Err(()) => return Err(format!("'{name}' is ambiguous")),
            Ok(Some(iu)) => return Ok(ExpressionResult::scalar(Box::new(Expr::IURef(iu)))),
            Ok(None) => {} // not found in columns → continue
        }

        // Argument? Walk the scope chain.
        let mut iter = Some(scope as *const BindingInfo);
        while let Some(s_ptr) = iter {
            let s = unsafe { &*s_ptr };
            if let Some(ArgInfo::Value {
                node,
                scope: arg_scope,
            }) = s.lookup_arg(&name)
            {
                let node = node.clone();
                let eval_scope: *const BindingInfo = match arg_scope {
                    Some(p) => p.as_ptr(),
                    None => scope as *const BindingInfo,
                };
                let eval_scope_ref = unsafe { &*eval_scope };
                let mut res = self.analyze_expression(eval_scope_ref, &node)?;
                if res.is_table() {
                    unsafe {
                        res.binding_mut().set_parent(scope);
                    }
                }
                return Ok(res);
            }
            iter = unsafe { s.parent() }.map(|p| p as *const BindingInfo);
        }

        // Let without arguments?
        if let Some(&idx) = self.let_lookup.get(&name) {
            if idx < self.let_scope_limit {
                if !self.lets[idx].sig.args.is_empty() {
                    return Err(format!("'{name}' is a function"));
                }
                let body = self.lets[idx].body.clone();
                let old_limit = self.let_scope_limit;
                self.let_scope_limit = idx;
                let res = BindingInfo::with_root_scope(|root| self.analyze_expression(root, &body));
                self.let_scope_limit = old_limit;
                return res;
            }
        }

        // Table scan?  A simple identifier is a single-part name.
        self.make_table_scan(scope, vec![name])
    }

    // ── member access: base.part ──────────────────────────────────────────

    /// Extract a dotted identifier path as a list of parts.
    /// Each element is one syntactic component, preserving dots inside
    /// quoted identifiers (e.g. `kafka . default . "my.topic.name"` →
    /// `["kafka", "default", "my.topic.name"]`).
    /// Returns `None` if the AST node is not a pure identifier chain.
    fn extract_dotted_parts(ast: &Ast) -> Option<Vec<String>> {
        match ast {
            Ast::Identifier(s) => Some(vec![s.clone()]),
            Ast::Access { base, part } => {
                let mut parts = Self::extract_dotted_parts(base)?;
                parts.push(part.clone());
                Some(parts)
            }
            _ => None,
        }
    }

    fn analyze_access(
        &mut self,
        scope: &BindingInfo,
        base_ast: &Ast,
        part: &str,
    ) -> Result<ExpressionResult, String> {
        let col_name = self.extract_symbol_str(scope, part);

        // Simple case: `table.column` where base is a plain identifier.
        if let Ast::Identifier(s) = base_ast {
            let base_name = self.extract_symbol_str(scope, s);
            return match scope.lookup_scoped(&base_name, &col_name) {
                Ok(Some(iu)) => Ok(ExpressionResult::scalar(Box::new(Expr::IURef(iu)))),
                Ok(None) => Err(format!("'{base_name}.{col_name}' not found")),
                Err(false) => Err(format!("'{col_name}' is ambiguous")),
                Err(true) => Err(format!("'{base_name}' is ambiguous")),
            };
        }

        // Qualified name: `catalog.schema.table` used as a relation reference.
        // Collect parts, preserving each component as an atomic unit.
        if let Some(mut parts) = Self::extract_dotted_parts(base_ast) {
            parts.push(col_name.clone());
            let lookup_key = parts.join(".");
            if self.schema.lookup_table(&lookup_key).is_some() {
                return BindingInfo::with_root_scope(|root| {
                    self.make_table_scan(root, parts)
                });
            }
        }

        Err(format!("invalid access to column '{col_name}'"))
    }

    // ── binary expressions ────────────────────────────────────────────────

    fn analyze_binary(
        &mut self,
        scope: &BindingInfo,
        op: &BinaryOp,
        left_ast: &Ast,
        right_ast: &Ast,
    ) -> Result<ExpressionResult, String> {
        let mut left = self.analyze_expression(scope, left_ast)?;
        let mut right = self.analyze_expression(scope, right_ast)?;

        let arithmetic = |l: &mut ExpressionResult,
                          r: &mut ExpressionResult,
                          op_name: &str,
                          bin_op: BinOp|
         -> Result<ExpressionResult, String> {
            if !l.is_scalar() || !r.is_scalar() {
                return Err(format!("scalar value required in operator '{op_name}'"));
            }
            let lt = l.expr_mut().typ();
            let rt = r.expr_mut().typ();
            if lt.is_numeric() && rt.is_numeric() {
                let result_type = (if lt.base < rt.base { rt } else { lt })
                    .with_nullable(lt.is_nullable() || rt.is_nullable());
                let le = std::mem::replace(
                    l.expr_mut(),
                    Box::new(Expr::IURef(IU::new(0, Type::unknown()))),
                );
                let re = std::mem::replace(
                    r.expr_mut(),
                    Box::new(Expr::IURef(IU::new(0, Type::unknown()))),
                );
                Ok(ExpressionResult::scalar(Box::new(Expr::Binary {
                    left: le,
                    right: re,
                    typ: result_type,
                    op: bin_op,
                })))
            } else if bin_op == BinOp::Plus && lt.is_string() && rt.is_string() {
                let result_type = Type::text().with_nullable(lt.is_nullable() || rt.is_nullable());
                let le = std::mem::replace(
                    l.expr_mut(),
                    Box::new(Expr::IURef(IU::new(0, Type::unknown()))),
                );
                let re = std::mem::replace(
                    r.expr_mut(),
                    Box::new(Expr::IURef(IU::new(0, Type::unknown()))),
                );
                Ok(ExpressionResult::scalar(Box::new(Expr::Binary {
                    left: le,
                    right: re,
                    typ: result_type,
                    op: BinOp::Concat,
                })))
            } else if matches!(lt.base, TypeBase::Date)
                && matches!(rt.base, TypeBase::Interval)
                && (bin_op == BinOp::Plus || bin_op == BinOp::Minus)
            {
                let result_type = Type::date().with_nullable(lt.is_nullable() || rt.is_nullable());
                let le = std::mem::replace(
                    l.expr_mut(),
                    Box::new(Expr::IURef(IU::new(0, Type::unknown()))),
                );
                let re = std::mem::replace(
                    r.expr_mut(),
                    Box::new(Expr::IURef(IU::new(0, Type::unknown()))),
                );
                Ok(ExpressionResult::scalar(Box::new(Expr::Binary {
                    left: le,
                    right: re,
                    typ: result_type,
                    op: bin_op,
                })))
            } else {
                Err(format!("'{op_name}' requires numerical arguments"))
            }
        };

        let comparison = |l: &mut ExpressionResult,
                          r: &mut ExpressionResult,
                          op_name: &str,
                          mode: CmpMode|
         -> Result<ExpressionResult, String> {
            if !l.is_scalar() || !r.is_scalar() {
                return Err(format!("scalar value required in operator '{op_name}'"));
            }
            enforce_comparable_exprs(l.expr_mut(), r.expr_mut())?;
            let collate = Collate;
            let le = take_expr(l);
            let re = take_expr(r);
            Ok(ExpressionResult::scalar(Box::new(Expr::Comparison {
                left: le,
                right: re,
                mode,
                collate,
            })))
        };

        let logical = |l: &mut ExpressionResult,
                       r: &mut ExpressionResult,
                       op_name: &str,
                       bin_op: BinOp|
         -> Result<ExpressionResult, String> {
            if !l.is_scalar() || !r.is_scalar() {
                return Err(format!("scalar value required in operator '{op_name}'"));
            }
            // Coerce Unknown → bool nullable
            if l.expr_mut().typ().base == TypeBase::Unknown {
                let e = take_expr(l);
                *l.expr_mut() = Box::new(Expr::Cast {
                    input: e,
                    typ: Type::bool_().as_nullable(),
                });
            }
            if r.expr_mut().typ().base == TypeBase::Unknown {
                let e = take_expr(r);
                *r.expr_mut() = Box::new(Expr::Cast {
                    input: e,
                    typ: Type::bool_().as_nullable(),
                });
            }
            let lt = l.expr_mut().typ();
            let rt = r.expr_mut().typ();
            if lt.base != TypeBase::Bool || rt.base != TypeBase::Bool {
                return Err(format!("'{op_name}' requires boolean arguments"));
            }
            let result_type = Type::bool_().with_nullable(lt.is_nullable() || rt.is_nullable());
            let le = take_expr(l);
            let re = take_expr(r);
            Ok(ExpressionResult::scalar(Box::new(Expr::Binary {
                left: le,
                right: re,
                typ: result_type,
                op: bin_op,
            })))
        };

        match op {
            BinaryOp::Plus => arithmetic(&mut left, &mut right, "+", BinOp::Plus),
            BinaryOp::Minus => arithmetic(&mut left, &mut right, "-", BinOp::Minus),
            BinaryOp::Mul => arithmetic(&mut left, &mut right, "*", BinOp::Mul),
            BinaryOp::Div => arithmetic(&mut left, &mut right, "/", BinOp::Div),
            BinaryOp::Mod => arithmetic(&mut left, &mut right, "%", BinOp::Mod),
            BinaryOp::Pow => arithmetic(&mut left, &mut right, "^", BinOp::Power),
            BinaryOp::Less => comparison(&mut left, &mut right, "<", CmpMode::Less),
            BinaryOp::Greater => comparison(&mut left, &mut right, ">", CmpMode::Greater),
            BinaryOp::Equals => comparison(&mut left, &mut right, "=", CmpMode::Equal),
            BinaryOp::NotEquals => comparison(&mut left, &mut right, "<>", CmpMode::NotEqual),
            BinaryOp::LessOrEqual => comparison(&mut left, &mut right, "<=", CmpMode::LessOrEqual),
            BinaryOp::GreaterOrEqual => {
                comparison(&mut left, &mut right, ">=", CmpMode::GreaterOrEqual)
            }
            BinaryOp::And => logical(&mut left, &mut right, "&&", BinOp::And),
            BinaryOp::Or => logical(&mut left, &mut right, "||", BinOp::Or),
        }
    }

    // ── unary expressions ─────────────────────────────────────────────────

    fn analyze_unary(
        &mut self,
        scope: &BindingInfo,
        op: &UnaryOp,
        value_ast: &Ast,
    ) -> Result<ExpressionResult, String> {
        let mut val = self.analyze_expression(scope, value_ast)?;
        if !val.is_scalar() {
            return Err("scalar value required in unary operator".into());
        }
        let vt = val.expr_mut().typ();
        match op {
            UnaryOp::Plus | UnaryOp::Minus => {
                if !vt.is_numeric() && vt.base != TypeBase::Interval {
                    return Err("unary +/- requires numerical argument".into());
                }
                let un_op = if matches!(op, UnaryOp::Plus) {
                    UnOp::Plus
                } else {
                    UnOp::Minus
                };
                let e = take_expr(&mut val);
                Ok(ExpressionResult::scalar(Box::new(Expr::Unary {
                    input: e,
                    typ: vt,
                    op: un_op,
                })))
            }
            UnaryOp::Not => {
                if vt.base != TypeBase::Bool {
                    return Err("! requires a boolean argument".into());
                }
                let e = take_expr(&mut val);
                Ok(ExpressionResult::scalar(Box::new(Expr::Unary {
                    input: e,
                    typ: vt,
                    op: UnOp::Not,
                })))
            }
        }
    }

    // ── cast ──────────────────────────────────────────────────────────────

    fn analyze_cast(
        &mut self,
        scope: &BindingInfo,
        value_ast: &Ast,
        type_ast: &crate::parser::ast::Type,
    ) -> Result<ExpressionResult, String> {
        let mut val = self.analyze_expression(scope, value_ast)?;
        if !val.is_scalar() {
            return Err("casts require scalar values".into());
        }
        let typ = parse_simple_type(match type_ast {
            crate::parser::ast::Type::Simple(n) => n,
            _ => return Err("invalid cast type".into()),
        })?;
        let ordering = val.ordering();
        let e = take_expr(&mut val);
        Ok(ExpressionResult::scalar_ord(
            Box::new(Expr::Cast { input: e, typ }),
            ordering,
        ))
    }

    // ── function call dispatch ────────────────────────────────────────────

    fn analyze_call(
        &mut self,
        scope: &BindingInfo,
        func_ast: &Ast,
        raw_args: &[FuncArg],
    ) -> Result<ExpressionResult, String> {
        // Determine if this is a method call (base.method) or free call (name)
        let (base_result, func_name) = match func_ast {
            Ast::Access { base, part } => {
                let b = self.analyze_expression(scope, base)?;
                (Some(b), part.clone())
            }
            Ast::Identifier(n) => (None, n.clone()),
            _ => return Err("invalid function name".into()),
        };

        // Resolve the signature
        let (sig, let_idx) = self.resolve_signature(&base_result, &func_name)?;

        // Bind positional and named arguments
        let bound = bind_args(&sig, raw_args, &func_name, scope, self)?;

        // Dispatch
        if let Some(idx) = let_idx {
            return self.call_let(scope, idx, &sig, bound);
        }

        self.call_builtin(scope, &func_name, base_result, &sig, bound)
    }

    fn resolve_signature(
        &self,
        base: &Option<ExpressionResult>,
        name: &str,
    ) -> Result<(Signature, Option<usize>), String> {
        if base.is_none() {
            // Free function: check lets, then built-ins
            if let Some(&idx) = self.let_lookup.get(name) {
                if idx < self.let_scope_limit {
                    return Ok((self.lets[idx].sig.clone(), Some(idx)));
                }
            }
            let sig =
                free_function_sig(name).ok_or_else(|| format!("function '{name}' not found"))?;
            return Ok((sig, None));
        }
        // Method call: use method table
        let sig = method_sig(base.as_ref().unwrap(), name).ok_or_else(|| {
            let type_name = if base.as_ref().unwrap().is_table() {
                "table".into()
            } else {
                base.as_ref()
                    .unwrap()
                    .binding()
                    .columns
                    .first()
                    .map(|_| "scalar")
                    .unwrap_or("scalar")
                    .to_string()
            };
            format!("'{name}' not found for '{type_name}'")
        })?;
        Ok((sig, None))
    }

    // ── let-binding call ──────────────────────────────────────────────────

    fn call_let(
        &mut self,
        outer_scope: &BindingInfo,
        idx: usize,
        sig: &Signature,
        bound: Vec<Option<Box<Ast>>>,
    ) -> Result<ExpressionResult, String> {
        let mut call_scope = BindingInfo::new();
        for (i, sig_arg) in sig.args.iter().enumerate() {
            let val = bound[i]
                .clone()
                .or_else(|| self.lets[idx].default_values[i].clone())
                .ok_or_else(|| format!("argument '{}' missing", sig_arg.name))?;
            match sig_arg.category {
                TypeCategory::Expression => {
                    call_scope.register_arg(sig_arg.name.clone(), val, None);
                }
                TypeCategory::Scalar | TypeCategory::Table => {
                    call_scope.register_arg(
                        sig_arg.name.clone(),
                        val,
                        Some(outer_scope as *const _),
                    );
                }
                TypeCategory::Symbol | TypeCategory::SymbolList => {
                    let sym = self.extract_symbol_from_ast(outer_scope, &val)?;
                    call_scope.register_symbol_arg(sig_arg.name.clone(), sym);
                }
                _ => return Err("unsupported argument category in let".into()),
            }
        }
        let body = self.lets[idx].body.clone();
        let old_limit = self.let_scope_limit;
        self.let_scope_limit = idx;
        let mut res = self.analyze_expression(&call_scope, &body)?;
        self.let_scope_limit = old_limit;
        if res.is_table() {
            res.binding_mut().parent_scope = None;
        }
        Ok(res)
    }

    // ── built-in call dispatch ────────────────────────────────────────────

    fn call_builtin(
        &mut self,
        scope: &BindingInfo,
        name: &str,
        base: Option<ExpressionResult>,
        sig: &Signature,
        bound: Vec<Option<Box<Ast>>>,
    ) -> Result<ExpressionResult, String> {
        // Helper: evaluate a scalar argument in scope
        let eval_scalar = |semana: &mut Self,
                           scope: &BindingInfo,
                           arg_name: &str,
                           ast_opt: &Option<Box<Ast>>|
         -> Result<ExpressionResult, String> {
            let ast = ast_opt
                .as_ref()
                .ok_or_else(|| format!("parameter '{arg_name}' missing in call to '{name}'"))?;
            let r = semana.analyze_expression(scope, ast)?;
            if !r.is_scalar() {
                return Err(format!(
                    "parameter '{arg_name}' requires a scalar in call to '{name}'"
                ));
            }
            Ok(r)
        };


        let eval_symbol = |semana: &mut Self,
                           scope: &BindingInfo,
                           arg_name: &str,
                           ast_opt: &Option<Box<Ast>>|
         -> Result<String, String> {
            let ast = ast_opt
                .as_ref()
                .ok_or_else(|| format!("parameter '{arg_name}' missing in call to '{name}'"))?;
            semana.extract_symbol_from_ast(scope, ast)
        };

        match name {
            // ── scalar method / ordering ──────────────────────────────────
            "asc" => {
                let mut b = base.unwrap();
                b.ordering_mut().mark_ascending();
                return Ok(b);
            }
            "desc" => {
                let mut b = base.unwrap();
                b.ordering_mut().mark_descending();
                return Ok(b);
            }

            // ── is (NULL-safe equality) ────────────────────────────────────
            "is" => {
                let b = base.unwrap();
                let mut b_scalar = if b.is_scalar() {
                    b
                } else {
                    return Err("'is' requires scalar".into());
                };
                let mut arg = eval_scalar(self, scope, &sig.args[0].name, &bound[0])?;
                enforce_comparable_exprs(b_scalar.expr_mut(), arg.expr_mut())?;
                let le = take_expr(&mut b_scalar);
                let re = take_expr(&mut arg);
                return Ok(ExpressionResult::scalar(Box::new(Expr::Comparison {
                    left: le,
                    right: re,
                    mode: CmpMode::Is,
                    collate: Collate,
                })));
            }

            // ── like ──────────────────────────────────────────────────────
            "like" => {
                let b = base.unwrap();
                let mut b_s = if b.is_scalar() {
                    b
                } else {
                    return Err("'like' requires a scalar base".into());
                };
                let mut arg = eval_scalar(self, scope, &sig.args[0].name, &bound[0])?;
                if !b_s.expr_mut().typ().is_string() || !arg.expr_mut().typ().is_string() {
                    return Err("'like' requires string arguments".into());
                }
                let le = take_expr(&mut b_s);
                let re = take_expr(&mut arg);
                return Ok(ExpressionResult::scalar(Box::new(Expr::Comparison {
                    left: le,
                    right: re,
                    mode: CmpMode::Like,
                    collate: Collate,
                })));
            }

            // ── between ───────────────────────────────────────────────────
            "between" => {
                let b = base.unwrap();
                let mut b_s = if b.is_scalar() {
                    b
                } else {
                    return Err("'between' requires a scalar base".into());
                };
                let mut lower = eval_scalar(self, scope, &sig.args[0].name, &bound[0])?;
                let mut upper = eval_scalar(self, scope, &sig.args[1].name, &bound[1])?;
                enforce_comparable_exprs(b_s.expr_mut(), lower.expr_mut())?;
                enforce_comparable_exprs(b_s.expr_mut(), upper.expr_mut())?;
                let base_e = take_expr(&mut b_s);
                let lower_e = take_expr(&mut lower);
                let upper_e = take_expr(&mut upper);
                return Ok(ExpressionResult::scalar(Box::new(Expr::Between {
                    base: base_e,
                    lower: lower_e,
                    upper: upper_e,
                    collate: Collate,
                })));
            }

            // ── in ────────────────────────────────────────────────────────
            "in" => {
                let b = base.unwrap();
                let mut b_s = if b.is_scalar() {
                    b
                } else {
                    return Err("'in' requires a scalar base".into());
                };
                let values_ast = bound[0].as_ref().ok_or("'in' missing values")?;
                let mut values = self.eval_scalar_list(scope, values_ast)?;
                if values.is_empty() {
                    return Ok(ExpressionResult::scalar(Box::new(Expr::Const {
                        value: Some("false".into()),
                        typ: Type::bool_(),
                    })));
                }
                let mut val_exprs = Vec::new();
                for v in &mut values {
                    enforce_comparable_exprs(b_s.expr_mut(), v.expr_mut())?;
                    val_exprs.push(take_expr(v));
                }
                let probe = take_expr(&mut b_s);
                return Ok(ExpressionResult::scalar(Box::new(Expr::In {
                    probe,
                    values: val_exprs,
                    collate: Collate,
                })));
            }

            // ── substr ────────────────────────────────────────────────────
            "substr" => {
                let b = base.unwrap();
                let mut b_s = if b.is_scalar() {
                    b
                } else {
                    return Err("'substr' requires scalar".into());
                };
                let from = if let Some(a) = &bound[0] {
                    let mut r = self.analyze_expression(scope, a)?;
                    if !r.is_scalar() || !r.expr_mut().typ().is_numeric() {
                        return Err("'substr' requires numeric arguments".into());
                    }
                    Some(take_expr(&mut r))
                } else {
                    None
                };
                let len = if let Some(a) = &bound[1] {
                    let mut r = self.analyze_expression(scope, a)?;
                    if !r.is_scalar() || !r.expr_mut().typ().is_numeric() {
                        return Err("'substr' requires numeric arguments".into());
                    }
                    Some(take_expr(&mut r))
                } else {
                    None
                };
                if from.is_none() && len.is_none() {
                    return Err("'substr' requires numeric arguments".into());
                }
                let value = take_expr(&mut b_s);
                return Ok(ExpressionResult::scalar(Box::new(Expr::Substr {
                    value,
                    from,
                    len,
                })));
            }

            // ── extract ───────────────────────────────────────────────────
            "extract" => {
                let b = base.unwrap();
                let mut b_s = if b.is_scalar() {
                    b
                } else {
                    return Err("'extract' requires scalar base".into());
                };
                let part_name = eval_symbol(self, scope, &sig.args[0].name, &bound[0])?;
                let part = match part_name.as_str() {
                    "year" => DatePart::Year,
                    "month" => DatePart::Month,
                    "day" => DatePart::Day,
                    other => return Err(format!("unknown date part '{other}'")),
                };
                let input = take_expr(&mut b_s);
                return Ok(ExpressionResult::scalar(Box::new(Expr::Extract {
                    input,
                    part,
                })));
            }

            // ── filter ────────────────────────────────────────────────────
            "filter" => {
                let b = base.unwrap();
                let (op, binding) = if b.is_table() {
                    b.into_parts()
                } else {
                    return Err("'filter' requires a table base".into());
                };
                let cond_ast = bound[0].as_ref().ok_or("'filter' missing condition")?;
                let mut cond = self.analyze_expression(&binding, cond_ast)?;
                if cond.expr_mut().typ().base != TypeBase::Bool {
                    return Err("'filter' requires a boolean filter condition".into());
                }
                let cond_e = take_expr(&mut cond);
                return Ok(ExpressionResult::table(
                    Box::new(Op::Select {
                        input: op,
                        condition: cond_e,
                    }),
                    binding,
                ));
            }

            // ── join ──────────────────────────────────────────────────────
            "join" => {
                let b = base.unwrap();
                return self.analyze_join(scope, b, sig, &bound);
            }

            // ── groupby ───────────────────────────────────────────────────
            "groupby" => {
                let b = base.unwrap();
                return self.analyze_groupby(b, &bound);
            }

            // ── aggregate ─────────────────────────────────────────────────
            "aggregate" => {
                let b = base.unwrap();
                return self.analyze_aggregate(b, &bound);
            }

            // ── distinct ──────────────────────────────────────────────────
            "distinct" => {
                let b = base.unwrap();
                return self.analyze_distinct(b);
            }

            // ── set operations ────────────────────────────────────────────
            "union" | "except" | "intersect" => {
                let b = base.unwrap();
                return self.analyze_set_op(scope, name, b, &bound);
            }

            // ── window ────────────────────────────────────────────────────
            "window" => {
                let b = base.unwrap();
                return self.analyze_window(b, &bound);
            }

            // ── orderby ───────────────────────────────────────────────────
            "orderby" => {
                let b = base.unwrap();
                return self.analyze_orderby(b, &bound);
            }

            // ── map / project / projectout ────────────────────────────────
            "map" => {
                let b = base.unwrap();
                return self.analyze_map(b, &bound, false);
            }
            "project" => {
                let b = base.unwrap();
                return self.analyze_map(b, &bound, true);
            }
            "projectout" => {
                let b = base.unwrap();
                return self.analyze_projectout(b, &bound);
            }

            // ── as / alias ────────────────────────────────────────────────
            "as" => {
                let mut b = base.unwrap();
                let new_name = eval_symbol(self, scope, &sig.args[0].name, &bound[0])?;
                if b.is_table() {
                    let binding = b.binding_mut();
                    let cols: HashMap<String, Option<Rc<IU>>> = binding.column_lookup.clone();
                    binding.scopes.clear();
                    let s = Scope {
                        columns: cols,
                        ambiguous: false,
                    };
                    binding.scopes.insert(new_name, s);
                }
                return Ok(b);
            }
            "alias" => {
                let mut b = base.unwrap();
                let new_name = eval_symbol(self, scope, &sig.args[0].name, &bound[0])?;
                if b.is_table() {
                    let binding = b.binding_mut();
                    let ius: Vec<Rc<IU>> =
                        binding.columns.iter().map(|c| Rc::clone(&c.iu)).collect();
                    binding.aliases.insert(
                        new_name,
                        Alias {
                            columns: ius,
                            ambiguous: false,
                        },
                    );
                }
                return Ok(b);
            }

            // ── aggregate functions (count / sum / avg / min / max) ────────
            "count" | "sum" | "avg" | "min" | "max" => {
                return self.handle_aggregate(scope, name, sig, &bound);
            }

            // ── window functions ──────────────────────────────────────────
            "row_number" | "rank" | "dense_rank" | "ntile" | "lead" | "lag" | "first_value"
            | "last_value" => {
                return self.handle_window(scope, name, sig, &bound);
            }

            // ── table construction ────────────────────────────────────────
            "table" => {
                let arg = bound[0].as_ref().ok_or("'table' missing values")?;
                return self.analyze_table_construction(scope, arg);
            }

            // ── case ──────────────────────────────────────────────────────
            "case" => {
                return self.analyze_case(scope, &bound);
            }

            // ── gensym ────────────────────────────────────────────────────
            "gensym" => {
                let base_name = bound[0]
                    .as_ref()
                    .and_then(|a| {
                        if let Ast::Identifier(s) = a.as_ref() {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or("sym");
                let sym = format!(" {} {}", base_name, self.next_symbol_id);
                self.next_symbol_id += 1;
                return Ok(ExpressionResult::scalar(Box::new(Expr::Const {
                    value: Some(sym),
                    typ: Type::text(),
                })));
            }

            // ── foreigncall ───────────────────────────────────────────────
            "foreigncall" => {
                return self.analyze_foreign_call(scope, sig, &bound);
            }

            _ => return Err(format!("builtin '{name}' not implemented")),
        }
    }

    // ── join ──────────────────────────────────────────────────────────────

    fn analyze_join(
        &mut self,
        outer_scope: &BindingInfo,
        input: ExpressionResult,
        _sig: &Signature,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        if !input.is_table() {
            return Err("'join' requires a table base".into());
        }

        // Determine join type
        let mut join_type = JoinType::Inner;
        let mut left_only = false;
        let mut right_only = false;
        if let Some(jt_ast) = &bound[2] {
            let jt = self.extract_symbol_from_ast(outer_scope, jt_ast)?;
            match jt.as_str() {
                "inner" => join_type = JoinType::Inner,
                "left" | "leftouter" => join_type = JoinType::LeftOuter,
                "right" | "rightouter" => join_type = JoinType::RightOuter,
                "full" | "fullouter" => join_type = JoinType::FullOuter,
                "leftsemi" | "exists" => {
                    join_type = JoinType::LeftSemi;
                    left_only = true;
                }
                "rightsemi" => {
                    join_type = JoinType::RightSemi;
                    right_only = true;
                }
                "leftanti" | "notexists" => {
                    join_type = JoinType::LeftAnti;
                    left_only = true;
                }
                "rightanti" => {
                    join_type = JoinType::RightAnti;
                    right_only = true;
                }
                other => return Err(format!("unknown join type '{other}'")),
            }
        }

        // Analyze the right table
        let table_ast = bound[0].as_ref().ok_or("'join' missing table argument")?;
        let other = self.analyze_expression(outer_scope, table_ast)?;
        if !other.is_table() {
            return Err("join 'table' argument must be a table".into());
        }

        // Build a merged binding for evaluating the condition
        let (input_op, input_binding) = input.into_parts();
        let (other_op, other_binding) = other.into_parts();

        let mut merged = input_binding.clone();
        merged.join(&other_binding);
        unsafe {
            merged.set_parent(outer_scope);
        }

        // Evaluate join condition in the merged binding
        let cond_ast = bound[1].as_ref().ok_or("'join' missing condition")?;
        let mut cond = self.analyze_expression(&merged, cond_ast)?;
        if !cond.is_scalar() || cond.expr_mut().typ().base != TypeBase::Bool {
            return Err("join condition must be a boolean".into());
        }
        let cond_e = take_expr(&mut cond);

        // Result binding: left-only for semi/anti-left, right-only for anti-right, merged for inner/outer
        let final_binding = if left_only {
            input_binding
        } else if right_only {
            other_binding
        } else {
            merged
        };

        Ok(ExpressionResult::table(
            Box::new(Op::Join {
                left: input_op,
                right: other_op,
                condition: cond_e,
                join_type,
            }),
            final_binding,
        ))
    }

    // ── groupby ───────────────────────────────────────────────────────────

    fn analyze_groupby(
        &mut self,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let (input_op, input_binding) = input.into_parts();

        let mut result_binding = BindingInfo::new();
        result_binding.parent_scope = input_binding.parent_scope;
        let scope_ptr = result_binding.add_scope("groupby");

        let mut group_by: Vec<MapEntry> = Vec::new();

        // Groups
        if let Some(g_ast) = &bound[0] {
            let groups = self.eval_expr_list(&input_binding, g_ast)?;
            for (col_name, mut er) in groups {
                if !er.is_scalar() {
                    return Err("groupby requires scalar groups".into());
                }
                let et = er.expr_mut().typ();
                let iu = self.new_iu(et);
                let name = if col_name.is_empty() {
                    (scope_ptr as usize).to_string() // placeholder; will fix below
                } else {
                    col_name
                };
                let n = if name.is_empty() {
                    result_binding.columns.len() + 1
                } else {
                    0
                };
                let final_name = if n > 0 { n.to_string() } else { name };
                let e = take_expr(&mut er);
                result_binding.add_binding(scope_ptr, final_name, Rc::clone(&iu));
                group_by.push(MapEntry {
                    value: e,
                    iu: Some(iu),
                });
            }
        }

        // Aggregates
        let gbs_rc = Rc::new(RefCell::new(GroupByState {
            pre_binding: input_binding.clone(),
            aggregations: Vec::new(),
            is_window: false,
        }));
        result_binding.gbs = Some(Rc::clone(&gbs_rc));

        // Collect aggregate result expressions; these are typically IURefs back to
        // aggregate IUs already pushed into `aggregations` by `handle_aggregate`.
        let mut agg_results: Vec<(String, Box<Expr>)> = Vec::new();
        if let Some(a_ast) = &bound[1] {
            let agg_list = self.eval_expr_list(&result_binding, a_ast)?;
            for (col_name, mut er) in agg_list {
                if !er.is_scalar() {
                    return Err("groupby requires scalar aggregates".into());
                }
                agg_results.push((col_name, take_expr(&mut er)));
            }
        }
        result_binding.gbs = None;

        let GroupByState { aggregations, .. } = Rc::try_unwrap(gbs_rc).ok().unwrap().into_inner();

        // TODO: grouping sets (bound[2], bound[3])
        if bound.len() > 2
            && (bound[2].is_some() || bound.get(3).and_then(|x| x.as_ref()).is_some())
        {
            return Err("grouping sets not implemented yet".into());
        }

        let mut tree: Box<Op> = Box::new(Op::GroupBy {
            input: input_op,
            group_by,
            aggregates: aggregations,
        });

        // For each aggregate result expression:
        //   - IURef(iu) → the IU is already in the GroupBy output; just register the name
        //   - complex expr → wrap in a Map with a new IU
        let mut map_computations: Vec<MapEntry> = Vec::new();
        for (col_name, expr) in agg_results {
            match expr.as_ref() {
                Expr::IURef(existing_iu) => {
                    result_binding.add_binding(scope_ptr, col_name, Rc::clone(existing_iu));
                }
                _ => {
                    let et = expr.typ();
                    let iu = self.new_iu(et);
                    result_binding.add_binding(scope_ptr, col_name, Rc::clone(&iu));
                    map_computations.push(MapEntry {
                        value: expr,
                        iu: Some(iu),
                    });
                }
            }
        }
        if !map_computations.is_empty() {
            tree = Box::new(Op::Map {
                input: tree,
                computations: map_computations,
            });
        }

        Ok(ExpressionResult::table(tree, result_binding))
    }

    // ── aggregate (scalar) ────────────────────────────────────────────────

    fn analyze_aggregate(
        &mut self,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let (input_op, input_binding) = input.into_parts();

        let mut result_binding = BindingInfo::new();
        result_binding.parent_scope = input_binding.parent_scope;

        let gbs_rc = Rc::new(RefCell::new(GroupByState {
            pre_binding: input_binding.clone(),
            aggregations: Vec::new(),
            is_window: false,
        }));
        result_binding.gbs = Some(Rc::clone(&gbs_rc));

        let agg_ast = bound[0].as_ref().ok_or("'aggregate' missing argument")?;
        let agg_list = self.eval_expr_list(&result_binding, agg_ast)?;
        let (_, mut result_er) = agg_list
            .into_iter()
            .next()
            .ok_or("'aggregate' requires at least one expression")?;
        if !result_er.is_scalar() {
            return Err("aggregate requires scalar aggregates".into());
        }
        let computation = take_expr(&mut result_er);

        result_binding.gbs = None;
        let GroupByState { aggregations, .. } = Rc::try_unwrap(gbs_rc).ok().unwrap().into_inner();

        let expr = Box::new(Expr::Aggregate {
            input: input_op,
            aggregates: aggregations,
            computation,
        });
        Ok(ExpressionResult::scalar(expr))
    }

    // ── distinct ──────────────────────────────────────────────────────────

    fn analyze_distinct(&mut self, input: ExpressionResult) -> Result<ExpressionResult, String> {
        let (input_op, input_binding) = input.into_parts();
        let mut result_binding = BindingInfo::new();
        result_binding.parent_scope = input_binding.parent_scope;
        let scope_ptr = result_binding.add_scope("distinct");
        let mut group_by = Vec::new();
        for col in &input_binding.columns {
            let iu = self.new_iu(col.iu.typ);
            result_binding.add_binding(scope_ptr, col.name.clone(), Rc::clone(&iu));
            group_by.push(MapEntry {
                value: Box::new(Expr::IURef(Rc::clone(&col.iu))),
                iu: Some(iu),
            });
        }
        let tree = Box::new(Op::GroupBy {
            input: input_op,
            group_by,
            aggregates: Vec::new(),
        });
        Ok(ExpressionResult::table(tree, result_binding))
    }

    // ── set operations ────────────────────────────────────────────────────

    fn analyze_set_op(
        &mut self,
        outer_scope: &BindingInfo,
        name: &str,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let all = if let Some(all_ast) = &bound[1] {
            match all_ast.as_ref() {
                Ast::Literal(Literal::True) => true,
                Ast::Literal(Literal::False) => false,
                _ => {
                    return Err(format!(
                        "'{name}' 'all' argument must be a boolean constant"
                    ))
                }
            }
        } else {
            false
        };

        let set_op = match (name, all) {
            ("union", false) => SetOp::Union,
            ("union", true) => SetOp::UnionAll,
            ("except", false) => SetOp::Except,
            ("except", true) => SetOp::ExceptAll,
            ("intersect", false) => SetOp::Intersect,
            ("intersect", true) => SetOp::IntersectAll,
            _ => unreachable!(),
        };

        let other_ast = bound[0]
            .as_ref()
            .ok_or_else(|| format!("'{name}' missing table argument"))?;
        let other = self.analyze_expression(outer_scope, other_ast)?;
        if !other.is_table() {
            return Err(format!("'{name}' table argument must be a table"));
        }

        if input.binding().columns.len() != other.binding().columns.len() {
            return Err(format!("'{name}' requires tables with identical schema"));
        }

        let mut result = BindingInfo::new();
        unsafe {
            result.set_parent(outer_scope);
        }
        let scope_ptr = result.add_scope(name);

        let mut left_cols = Vec::new();
        let mut right_cols = Vec::new();
        let mut result_ius = Vec::new();

        for (i, (lc, rc)) in input
            .binding()
            .columns
            .iter()
            .zip(other.binding().columns.iter())
            .enumerate()
        {
            let t1 = lc.iu.typ;
            let t2 = rc.iu.typ;
            if t1.with_nullable(true) != t2.with_nullable(true) {
                return Err(format!(
                    "'{name}' requires tables with identical schema. Mismatch in column {i}"
                ));
            }
            left_cols.push(Box::new(Expr::IURef(Rc::clone(&lc.iu))));
            right_cols.push(Box::new(Expr::IURef(Rc::clone(&rc.iu))));
            let result_iu = self.new_iu(t1.with_nullable(t1.is_nullable() || t2.is_nullable()));
            result.add_binding(scope_ptr, lc.name.clone(), Rc::clone(&result_iu));
            result_ius.push(result_iu);
        }

        let (left_op, _) = input.into_parts();
        let (right_op, _) = other.into_parts();
        let op = Box::new(Op::SetOperation {
            left: left_op,
            right: right_op,
            left_cols,
            right_cols,
            result_cols: result_ius,
            op: set_op,
        });
        Ok(ExpressionResult::table(op, result))
    }

    // ── window ────────────────────────────────────────────────────────────

    fn analyze_window(
        &mut self,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        if bound.len() > 3
            && (bound[3].is_some()
                || bound.get(4).map_or(false, |x| x.is_some())
                || bound.get(5).map_or(false, |x| x.is_some()))
        {
            return Err("frames not implemented yet".into());
        }

        let (input_op, input_binding) = input.into_parts();
        let mut result_binding = input_binding.clone();

        let gbs_rc = Rc::new(RefCell::new(GroupByState {
            pre_binding: input_binding.clone(),
            aggregations: Vec::new(),
            is_window: true,
        }));
        result_binding.gbs = Some(Rc::clone(&gbs_rc));

        // Evaluate window expressions; these are IURefs to the window aggregate IUs.
        let mut col_names: Vec<String> = Vec::new();
        if let Some(expr_ast) = &bound[0] {
            let exprs = self.eval_expr_list(&result_binding, expr_ast)?;
            for (col_name, mut er) in exprs {
                if !er.is_scalar() {
                    return Err("window requires scalar aggregates".into());
                }
                col_names.push(col_name);
                let _ = take_expr(&mut er); // consume
            }
        }
        result_binding.gbs = None;

        let GroupByState { aggregations, .. } = Rc::try_unwrap(gbs_rc).ok().unwrap().into_inner();

        // Register window aggregate IUs in result_binding under their column names.
        let scope_ptr = result_binding.add_scope("window");
        for (i, col_name) in col_names.iter().enumerate() {
            if i < aggregations.len() {
                let iu = Rc::clone(&aggregations[i].iu);
                result_binding.add_binding(scope_ptr, col_name.clone(), iu);
            }
        }

        // Partition-by
        let mut partition_by = Vec::new();
        if let Some(pb_ast) = &bound[1] {
            let exprs = self.eval_expr_list(&input_binding, pb_ast)?;
            for (_, mut er) in exprs {
                if !er.is_scalar() {
                    return Err("partitionby requires scalar values".into());
                }
                partition_by.push(take_expr(&mut er));
            }
        }

        // Order-by
        let mut order_by = Vec::new();
        if let Some(ob_ast) = &bound[2] {
            let exprs = self.eval_expr_list(&input_binding, ob_ast)?;
            for (_, er) in exprs {
                if !er.is_scalar() {
                    return Err("orderby requires scalar values".into());
                }
                let ord = er.ordering();
                let e = er.expr();
                order_by.push(SortEntry {
                    value: e,
                    collate: Collate,
                    descending: ord.descending,
                });
            }
        }

        let tree = Box::new(Op::Window {
            input: input_op,
            aggregates: aggregations,
            partition_by,
            order_by,
        });
        // The Window node already emits all aggregate IUs; no extra Map needed.
        Ok(ExpressionResult::table(tree, result_binding))
    }

    // ── orderby ───────────────────────────────────────────────────────────

    fn analyze_orderby(
        &mut self,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let (input_op, input_binding) = input.into_parts();

        let mut order = Vec::new();
        if let Some(exprs_ast) = &bound[0] {
            let exprs = self.eval_expr_list(&input_binding, exprs_ast)?;
            for (_, er) in exprs {
                if !er.is_scalar() {
                    return Err("orderby requires scalar values".into());
                }
                let ord = er.ordering();
                let e = er.expr();
                order.push(SortEntry {
                    value: e,
                    collate: Collate,
                    descending: ord.descending,
                });
            }
        }

        let limit = if let Some(a) = &bound[1] {
            Some(extract_integer_const(a)?)
        } else {
            None
        };
        let offset = if let Some(a) = &bound[2] {
            Some(extract_integer_const(a)?)
        } else {
            None
        };

        let op = Box::new(Op::Sort {
            input: input_op,
            order,
            limit,
            offset,
        });
        Ok(ExpressionResult::table(op, input_binding))
    }

    // ── map / project ─────────────────────────────────────────────────────

    fn analyze_map(
        &mut self,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
        project: bool,
    ) -> Result<ExpressionResult, String> {
        let (input_op, input_binding) = input.into_parts();

        let exprs_ast = bound[0].as_ref().ok_or("'map' missing expressions")?;
        let exprs = self.eval_expr_list(&input_binding, exprs_ast)?;

        let mut results: Vec<(String, Box<Expr>, Rc<IU>)> = Vec::new();
        for (col_name, mut er) in exprs {
            if !project {
                // columns are processed in the right order
                println!("Map column {col_name}");
            }
            if !er.is_scalar() {
                return Err("map requires scalar values".into());
            }
            let et = er.expr_mut().typ();
            let iu = self.new_iu(et);
            let id = iu.id;
            let typ = iu.typ;
            println!("Created IU {id:?}/{typ:?}");
            results.push((col_name, take_expr(&mut er), iu));
        }

        let mut result_binding = BindingInfo::new();
        result_binding.parent_scope = input_binding.parent_scope;
        if !project {
            result_binding = input_binding;
        }
        let scope_ptr = result_binding.add_scope(if project { "project" } else { "map" });

        let mut computations: Vec<MapEntry> = Vec::new();
        for (col_name, expr, iu) in results {
            // If the expression is a pass-through IURef, no new computation is needed
            let binding_iu = if let Expr::IURef(existing) = expr.as_ref() {
                result_binding.add_binding(scope_ptr, col_name, Rc::clone(existing));
                // No map entry needed
                continue;
            } else {
                Rc::clone(&iu)
            };
            result_binding.add_binding(scope_ptr, col_name, Rc::clone(&binding_iu));
            computations.push(MapEntry {
                value: expr,
                iu: Some(iu),
            });
        }

        let mut tree = input_op; // was moved out
        if !computations.is_empty() {
            if project {
                // Insert Map below any Sort at the top
                tree = insert_map_below_sort(tree, computations);
            } else {
                println!("Computations {computations:?}");
                tree = Box::new(Op::Map {
                    input: tree,
                    computations,
                });
            }
        }

        Ok(ExpressionResult::table(tree, result_binding))
    }

    // ── projectout ────────────────────────────────────────────────────────

    fn analyze_projectout(
        &mut self,
        input: ExpressionResult,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let (input_op, mut input_binding) = input.into_parts();

        let exprs_ast = bound[0].as_ref().ok_or("'projectout' missing columns")?;
        let exprs = self.eval_expr_list(&input_binding, exprs_ast)?;

        let mut to_remove: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (_, mut er) in exprs {
            if !er.is_scalar() {
                return Err("projectout requires scalar values".into());
            }
            match er.expr_mut().as_ref() {
                Expr::IURef(iu) => {
                    to_remove.insert(iu.id);
                }
                _ => return Err("projectout requires column references".into()),
            }
        }

        input_binding
            .columns
            .retain(|c| !to_remove.contains(&c.iu.id));
        input_binding
            .column_lookup
            .retain(|_, v| v.as_ref().map_or(true, |iu| !to_remove.contains(&iu.id)));
        for scope in input_binding.scopes.values_mut() {
            scope
                .columns
                .retain(|_, v| v.as_ref().map_or(true, |iu| !to_remove.contains(&iu.id)));
        }

        Ok(ExpressionResult::table(input_op, input_binding))
    }

    // ── case ──────────────────────────────────────────────────────────────

    fn analyze_case(
        &mut self,
        scope: &BindingInfo,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let cases_ast = bound[0].as_ref().ok_or("'case' missing cases")?;
        let cases_list = self.eval_funcarg_named_list(scope, cases_ast)?;

        let mut cases: Vec<(Box<Expr>, Box<Expr>)> = Vec::new();
        for (key_opt, val_ast) in cases_list {
            let key_ast = key_opt.ok_or("case requires cases of the form 'a => b'")?;
            let mut key_er = self.analyze_expression(scope, &key_ast)?;
            if !key_er.is_scalar() {
                return Err("case requires a scalar case value".into());
            }
            let mut val_er = self.analyze_expression(scope, &val_ast)?;
            if !val_er.is_scalar() {
                return Err("case requires a scalar case result".into());
            }
            cases.push((take_expr(&mut key_er), take_expr(&mut val_er)));
        }
        if cases.is_empty() {
            return Err("case requires a list of cases".into());
        }

        let result_typ = cases[0].1.typ();
        let nullable = cases.iter().any(|(_, v)| v.typ().is_nullable());

        let default: Box<Expr> = if let Some(else_ast) = &bound[1] {
            let mut er = self.analyze_expression(scope, else_ast)?;
            if !er.is_scalar() {
                return Err("case 'else' must be scalar".into());
            }
            take_expr(&mut er)
        } else {
            Box::new(Expr::Const {
                value: None,
                typ: result_typ.as_nullable(),
            })
        };

        // Coerce case results and default to a common type
        let final_type = result_typ.with_nullable(nullable || default.typ().is_nullable());
        for (_, v) in &mut cases {
            if v.typ().with_nullable(true) != final_type.with_nullable(true) {
                let old = std::mem::replace(v, Box::new(dummy_expr()));
                *v = Box::new(Expr::Cast {
                    input: old,
                    typ: final_type,
                });
            }
        }
        let default = if default.typ().with_nullable(true) != final_type.with_nullable(true) {
            Box::new(Expr::Cast {
                input: default,
                typ: final_type,
            })
        } else {
            default
        };

        // Searched vs simple case
        if let Some(search_ast) = &bound[2] {
            let mut search_er = self.analyze_expression(scope, search_ast)?;
            if !search_er.is_scalar() {
                return Err("case 'search' must be scalar".into());
            }
            let search = take_expr(&mut search_er);
            for (k, _) in &mut cases {
                enforce_comparable_exprs(&mut Box::new(Expr::IURef(IU::new(0, search.typ()))), k)?;
                // rough check
            }
            Ok(ExpressionResult::scalar(Box::new(Expr::SimpleCase {
                value: search,
                cases,
                default,
            })))
        } else {
            for (k, _) in &cases {
                if k.typ().base != TypeBase::Bool {
                    return Err("case requires boolean case conditions".into());
                }
            }
            Ok(ExpressionResult::scalar(Box::new(Expr::SearchedCase {
                cases,
                default,
            })))
        }
    }

    // ── table construction ────────────────────────────────────────────────

    fn analyze_table_construction(
        &mut self,
        scope: &BindingInfo,
        arg: &Ast,
    ) -> Result<ExpressionResult, String> {
        // arg must be a list of row-lists
        let rows = match arg {
            Ast::Literal(_) | Ast::Identifier(_) => vec![vec![(None::<String>, arg.clone())]],
            _ => self.extract_table_rows(scope, arg)?,
        };

        if rows.is_empty() {
            return Err("'table' requires at least one row".into());
        }

        let ncols = rows[0].len();
        let col_names: Vec<String> = rows[0]
            .iter()
            .enumerate()
            .map(|(i, (name, _))| name.clone().unwrap_or_else(|| (i + 1).to_string()))
            .collect();

        let mut values: Vec<Box<Expr>> = Vec::new();
        let mut col_types: Vec<Type> = vec![Type::unknown(); ncols];

        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != ncols {
                return Err(if row.len() < ncols {
                    "too few column values in inline table".into()
                } else {
                    "too many column values in inline table".into()
                });
            }
            for (col_idx, (_, val_ast)) in row.iter().enumerate() {
                let mut er = self.analyze_expression(scope, val_ast)?;
                if !er.is_scalar() {
                    return Err("inline tables require scalar values".into());
                }
                let typ = er.expr_mut().typ();
                if row_idx == 0 {
                    col_types[col_idx] = typ;
                } else if col_types[col_idx].base == TypeBase::Unknown {
                    col_types[col_idx] = typ.as_nullable();
                }
                values.push(take_expr(&mut er));
            }
        }

        // Add casts for type mismatches
        let row_count = rows.len();
        for row in 0..row_count {
            for col in 0..ncols {
                let e = &mut values[row * ncols + col];
                if e.typ().base != col_types[col].base {
                    let old = std::mem::replace(e, Box::new(dummy_expr()));
                    *e = Box::new(Expr::Cast {
                        input: old,
                        typ: col_types[col].with_nullable(e.typ().is_nullable()),
                    });
                }
            }
        }

        let mut binding = BindingInfo::new();
        let scope_ptr = binding.add_scope("table");
        let mut columns = Vec::new();
        for (i, typ) in col_types.iter().enumerate() {
            let iu = self.new_iu(*typ);
            binding.add_binding(scope_ptr, col_names[i].clone(), Rc::clone(&iu));
            columns.push(Rc::clone(&iu));
        }

        Ok(ExpressionResult::table(
            Box::new(Op::InlineTable {
                columns,
                values,
                row_count,
            }),
            binding,
        ))
    }

    fn extract_table_rows(
        &self,
        _scope: &BindingInfo,
        arg: &Ast,
    ) -> Result<Vec<Vec<(Option<String>, Ast)>>, String> {
        // `arg` is the synthetic __exprlist__ node produced by flatten_func_arg_value from
        // a FuncArg::List.  Each element is either:
        //   - FuncArg::List { items: [...] }  → a row (sub-list of column values)
        //   - FuncArg::Flat { value, .. }     → a single-column row
        let top_args = match arg {
            Ast::Call { func, args } if matches!(func.as_ref(), Ast::Identifier(n) if n == "__exprlist__") => {
                args
            }
            _ => return Err("table() requires a list of rows".into()),
        };

        let mut rows: Vec<Vec<(Option<String>, Ast)>> = Vec::new();
        for fa in top_args {
            match fa {
                FuncArg::List { items, .. } => {
                    let row: Vec<(Option<String>, Ast)> = items
                        .iter()
                        .map(|item| match item {
                            FuncArgNamed::Flat { name, value } => (name.clone(), *value.clone()),
                            FuncArgNamed::Case { .. } => (None, Ast::Literal(Literal::Null)),
                            FuncArgNamed::List { .. } => (None, Ast::Literal(Literal::Null)),
                        })
                        .collect();
                    rows.push(row);
                }
                FuncArg::Flat { name, value } => {
                    rows.push(vec![(name.clone(), *value.clone())]);
                }
            }
        }
        Ok(rows)
    }

    // ── foreigncall ───────────────────────────────────────────────────────

    fn analyze_foreign_call(
        &mut self,
        scope: &BindingInfo,
        _sig: &Signature,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let fn_name_ast = bound[0].as_ref().ok_or("foreigncall missing 'name'")?;
        let fn_name = match fn_name_ast.as_ref() {
            Ast::Literal(Literal::String(s)) => s.clone(),
            _ => return Err("foreigncall 'name' must be a string literal".into()),
        };

        let return_type_sym = self.extract_symbol_from_ast(
            scope,
            bound[1].as_ref().ok_or("foreigncall missing 'returns'")?,
        )?;
        let return_type = parse_simple_type(&return_type_sym)?;

        let mut args = Vec::new();
        if let Some(args_ast) = &bound[2] {
            let exprs = self.eval_expr_list(scope, args_ast)?;
            for (_, mut er) in exprs {
                if !er.is_scalar() {
                    return Err("foreigncall arguments must be scalar".into());
                }
                args.push(take_expr(&mut er));
            }
        }

        let call_type = if let Some(type_ast) = &bound[3] {
            let t = self.extract_symbol_from_ast(scope, type_ast)?;
            match t.as_str() {
                "function" => CallType::Function,
                "operator" | "leftassoc" => CallType::LeftAssoc,
                "rightassoc" => CallType::RightAssoc,
                other => return Err(format!("unknown foreigncall call type '{other}'")),
            }
        } else {
            CallType::Function
        };

        if matches!(call_type, CallType::LeftAssoc | CallType::RightAssoc) && args.len() < 2 {
            return Err("foreigncall with operator type requires at least two arguments".into());
        }

        Ok(ExpressionResult::scalar(Box::new(Expr::ForeignCall {
            name: fn_name,
            typ: return_type,
            args,
            call_type,
        })))
    }

    // ── aggregate / window function handling ──────────────────────────────

    fn handle_aggregate(
        &mut self,
        scope: &BindingInfo,
        name: &str,
        _sig: &Signature,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let gbs_opt: Option<Rc<RefCell<GroupByState>>> = scope.gbs.clone();
        let gbs = gbs_opt.ok_or_else(|| {
            format!("aggregate '{name}' can only be used in group by computations")
        })?;

        let (op, distinct_op) = match name {
            "count" => {
                if bound[0].is_some() {
                    (AggOp::Count, AggOp::CountDistinct)
                } else {
                    // count(*) — no distinct option
                    let mut state = gbs.borrow_mut();
                    let iu = self.new_iu(Type::integer());
                    state.aggregations.push(Aggregation {
                        value: None,
                        iu: Rc::clone(&iu),
                        op: AggOp::CountStar,
                        params: Vec::new(),
                    });
                    return Ok(ExpressionResult::scalar(Box::new(Expr::IURef(iu))));
                }
            }
            "sum" => (AggOp::Sum, AggOp::SumDistinct),
            "avg" => (AggOp::Avg, AggOp::AvgDistinct),
            "min" => (AggOp::Min, AggOp::Min),
            "max" => (AggOp::Max, AggOp::Max),
            _ => unreachable!(),
        };

        // Check for distinct
        let final_op = if bound.len() > 1 {
            if let Some(d_ast) = &bound[1] {
                if matches!(d_ast.as_ref(), Ast::Literal(Literal::True)) {
                    distinct_op
                } else {
                    op
                }
            } else {
                op
            }
        } else {
            op
        };

        let pre_binding = gbs.borrow().pre_binding.clone();
        let val_ast = bound[0]
            .as_ref()
            .ok_or_else(|| format!("aggregate '{name}' missing value argument"))?;
        let mut val = self.analyze_expression(&pre_binding, val_ast)?;
        if !val.is_scalar() {
            return Err(format!("aggregate '{name}' requires a scalar argument"));
        }
        let vt = val.expr_mut().typ();
        if !matches!(final_op, AggOp::Min | AggOp::Max) && !vt.is_numeric() {
            return Err(format!("aggregate '{name}' requires a numerical argument"));
        }
        let result_type = if matches!(final_op, AggOp::Count | AggOp::CountDistinct) {
            Type::integer()
        } else {
            vt
        };

        let iu = self.new_iu(result_type);
        let e = take_expr(&mut val);
        gbs.borrow_mut().aggregations.push(Aggregation {
            value: Some(e),
            iu: Rc::clone(&iu),
            op: final_op,
            params: Vec::new(),
        });
        Ok(ExpressionResult::scalar(Box::new(Expr::IURef(iu))))
    }

    fn handle_window(
        &mut self,
        scope: &BindingInfo,
        name: &str,
        _sig: &Signature,
        bound: &[Option<Box<Ast>>],
    ) -> Result<ExpressionResult, String> {
        let gbs_opt = scope.gbs.clone();
        let gbs = gbs_opt.as_ref().ok_or_else(|| {
            format!("window function '{name}' can only be used in window computations")
        })?;
        if !gbs.borrow().is_window {
            return Err(format!(
                "window function '{name}' can only be used in window computations"
            ));
        }

        let (op, result_type, needs_value): (AggOp, Type, bool) = match name {
            "row_number" => (AggOp::RowNumber, Type::integer(), false),
            "rank" => (AggOp::Rank, Type::integer(), true),
            "dense_rank" => (AggOp::DenseRank, Type::integer(), true),
            "ntile" => (AggOp::NTile, Type::integer(), true),
            "lead" => {
                let t = self.infer_window_value_type(scope, name, &bound[0])?;
                (AggOp::Lead, t, true)
            }
            "lag" => {
                let t = self.infer_window_value_type(scope, name, &bound[0])?;
                (AggOp::Lag, t, true)
            }
            "first_value" => {
                let t = self.infer_window_value_type(scope, name, &bound[0])?;
                (AggOp::FirstValue, t, true)
            }
            "last_value" => {
                let t = self.infer_window_value_type(scope, name, &bound[0])?;
                (AggOp::LastValue, t, true)
            }
            _ => unreachable!(),
        };

        let pre_binding = gbs.borrow().pre_binding.clone();

        let (value, mut params) = if needs_value {
            let val_ast = bound[0]
                .as_ref()
                .ok_or_else(|| format!("window function '{name}' missing value argument"))?;
            let mut val = self.analyze_expression(&pre_binding, val_ast)?;
            if !val.is_scalar() {
                return Err(format!(
                    "window function '{name}' requires a scalar argument"
                ));
            }
            let e = take_expr(&mut val);
            (Some(e), Vec::new())
        } else {
            (None, Vec::new())
        };

        // lead/lag optional offset and default
        if matches!(op, AggOp::Lead | AggOp::Lag) {
            let offset = if bound.len() > 1 && bound[1].is_some() {
                let ast = bound[1].as_ref().unwrap();
                let mut er = self.analyze_expression(&pre_binding, ast)?;
                if !er.is_scalar() || er.expr_mut().typ().base != TypeBase::Integer {
                    return Err("lead/lag offset must be an integer".into());
                }
                take_expr(&mut er)
            } else {
                Box::new(Expr::Const {
                    value: Some("1".into()),
                    typ: Type::integer(),
                })
            };
            params.push(offset);

            let default = if bound.len() > 2 && bound[2].is_some() {
                let ast = bound[2].as_ref().unwrap();
                let mut er = self.analyze_expression(&pre_binding, ast)?;
                if !er.is_scalar() {
                    return Err("lead/lag default must be scalar".into());
                }
                take_expr(&mut er)
            } else {
                Box::new(Expr::Const {
                    value: None,
                    typ: result_type.as_nullable(),
                })
            };
            params.push(default);
        }

        let iu = self.new_iu(result_type);
        gbs.borrow_mut().aggregations.push(Aggregation {
            value,
            iu: Rc::clone(&iu),
            op,
            params,
        });
        Ok(ExpressionResult::scalar(Box::new(Expr::IURef(iu))))
    }

    fn infer_window_value_type(
        &mut self,
        scope: &BindingInfo,
        name: &str,
        val_ast_opt: &Option<Box<Ast>>,
    ) -> Result<Type, String> {
        let gbs = scope
            .gbs
            .as_ref()
            .ok_or_else(|| format!("'{name}' used outside window context"))?;
        let pre_binding = gbs.borrow().pre_binding.clone();
        let val_ast = val_ast_opt
            .as_ref()
            .ok_or_else(|| format!("'{name}' missing value argument"))?;
        let mut er = self.analyze_expression(&pre_binding, val_ast)?;
        Ok(er.expr_mut().typ())
    }

    // ── expression list evaluation ────────────────────────────────────────

    /// Evaluate an argument that should produce a list of (name, expr) pairs.
    fn eval_expr_list(
        &mut self,
        scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<Vec<(String, ExpressionResult)>, String> {
        // The AST here is the VALUE of a FuncArg (either the Ast directly for a
        // single expression, or a FuncArg::List's items).
        // In our grammar, an expression-list argument arrives as a FuncArg which
        // the caller has already unwrapped to just its value ast.
        // We call this with the raw Ast from bound[i].
        //
        // Strategy: if the ast is a Literal or Identifier or expression, treat
        // it as a single item. For our FuncArg::List case, we need to descend.
        // But because bound[] already contains the value Ast (stripped of
        // FuncArg wrapper), we need special handling.
        //
        // Actually, expression list arguments arrive wrapped in a synthetic
        // FuncArgListAst which we parse in bind_args. Let's just evaluate:
        self.eval_expr_list_inner(scope, ast)
    }

    fn eval_expr_list_inner(
        &mut self,
        scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<Vec<(String, ExpressionResult)>, String> {
        // Check if ast is a synthetic ExprList node from bind_args
        // For now, treat non-list asts as single expressions
        match ast {
            Ast::Call { func, args } if matches!(func.as_ref(), Ast::Identifier(n) if n == "__exprlist__") =>
            {
                // Synthetic expression list node
                let mut results = Vec::new();
                for fa in args {
                    match fa {
                        FuncArg::Flat { name, value } => {
                            // Check alias BEFORE evaluate (alias resolution must bypass analyze_expression)
                            if let Some(alias_ius) = self.check_alias(scope, value) {
                                let col_names: HashMap<u64, String> = scope
                                    .columns
                                    .iter()
                                    .map(|c| (c.iu.id, c.name.clone()))
                                    .collect();
                                for iu in alias_ius {
                                    let n = col_names.get(&iu.id).cloned().unwrap_or_default();
                                    results.push((
                                        n,
                                        ExpressionResult::scalar(Box::new(Expr::IURef(iu))),
                                    ));
                                }
                            } else {
                                let er = self.analyze_expression(scope, value)?;
                                // Resolve explicit column names through symbol args (e.g. gensym names)
                                let col_name = if let Some(n) = name {
                                    self.extract_symbol_str(scope, n)
                                } else {
                                    infer_name(value)
                                };
                                results.push((col_name, er));
                            }
                        }
                        FuncArg::List { .. } => {
                            return Err("nested expression list not allowed here".into());
                        }
                    }
                }
                Ok(results)
            }
            _ => {
                // Single expression
                let name = infer_name(ast);
                let er = self.analyze_expression(scope, ast)?;
                Ok(vec![(name, er)])
            }
        }
    }

    fn check_alias(&self, scope: &BindingInfo, ast: &Ast) -> Option<Vec<Rc<IU>>> {
        let raw_name = match ast {
            Ast::Identifier(n) => n.as_str(),
            _ => return None,
        };
        // Resolve through symbol args (e.g., gensym-generated names)
        let name = self.extract_symbol_str(scope, raw_name);
        if scope.column_lookup.contains_key(name.as_str()) {
            return None;
        }
        scope.aliases.get(name.as_str()).and_then(|a| {
            if a.ambiguous {
                None
            } else {
                Some(a.columns.clone())
            }
        })
    }

    fn eval_scalar_list(
        &mut self,
        scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<Vec<ExpressionResult>, String> {
        let exprs = self.eval_expr_list(scope, ast)?;
        for (_, ref er) in &exprs {
            if !er.is_scalar() {
                return Err("expected scalar values in list".into());
            }
        }
        Ok(exprs.into_iter().map(|(_, er)| er).collect())
    }

    fn eval_funcarg_named_list(
        &mut self,
        _scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<Vec<(Option<Ast>, Ast)>, String> {
        // Returns (key_ast_opt, value_ast) for Case entries (key => value)
        match ast {
            Ast::Call { func, args } if matches!(func.as_ref(), Ast::Identifier(n) if n == "__exprlist__") =>
            {
                let mut result = Vec::new();
                for fa in args {
                    match fa {
                        FuncArg::Flat { name: _, value } => {
                            // Check if this is a synthetic __case_entry__(key, value) node
                            if let Ast::Call { func: ef, args: ea } = value.as_ref() {
                                if matches!(ef.as_ref(), Ast::Identifier(n) if n == "__case_entry__")
                                {
                                    if ea.len() == 2 {
                                        let key_ast = match &ea[0] {
                                            FuncArg::Flat { value: v, .. } => *v.clone(),
                                            _ => return Err("malformed __case_entry__".into()),
                                        };
                                        let val_ast = match &ea[1] {
                                            FuncArg::Flat { value: v, .. } => *v.clone(),
                                            _ => return Err("malformed __case_entry__".into()),
                                        };
                                        result.push((Some(key_ast), val_ast));
                                        continue;
                                    }
                                }
                            }
                            result.push((None, *value.clone()));
                        }
                        FuncArg::List { .. } => {
                            return Err("unexpected list in case argument".into())
                        }
                    }
                }
                Ok(result)
            }
            _ => Ok(vec![(None, ast.clone())]),
        }
    }

    // ── symbol extraction ─────────────────────────────────────────────────

    fn extract_symbol(&self, scope: &BindingInfo, ast: &Ast) -> Result<String, String> {
        let name = match ast {
            Ast::Identifier(n) => n.clone(),
            _ => return Err("expected identifier".into()),
        };
        Ok(self.extract_symbol_str(scope, &name))
    }

    fn extract_symbol_str(&self, scope: &BindingInfo, name: &str) -> String {
        // Walk the scope chain for symbol argument overrides
        let mut iter = Some(scope as *const BindingInfo);
        while let Some(s_ptr) = iter {
            let s = unsafe { &*s_ptr };
            if let Some(ArgInfo::Symbol(sym)) = s.lookup_arg(name) {
                return sym.clone();
            }
            iter = unsafe { s.parent() }.map(|p| p as *const BindingInfo);
        }
        name.to_string()
    }

    fn extract_symbol_from_ast(
        &mut self,
        scope: &BindingInfo,
        ast: &Ast,
    ) -> Result<String, String> {
        // Check for gensym
        if let Some(sym) = self.recognize_gensym(ast) {
            return Ok(sym);
        }
        self.extract_symbol(scope, ast)
    }

    fn recognize_gensym(&mut self, ast: &Ast) -> Option<String> {
        let (func, args) = match ast {
            Ast::Call { func, args } => (func, args),
            _ => return None,
        };
        match func.as_ref() {
            Ast::Identifier(n) if n == "gensym" => {}
            _ => return None,
        };
        let base = if args.is_empty() {
            "sym".to_string()
        } else if args.len() == 1 {
            match &args[0] {
                FuncArg::Flat { value, .. } => match value.as_ref() {
                    Ast::Identifier(s) => s.clone(),
                    _ => return None,
                },
                _ => return None,
            }
        } else {
            return None;
        };

        let sym = format!(" {} {}", base, self.next_symbol_id);
        self.next_symbol_id += 1;
        Some(sym)
    }
}

// ── argument binding (position / name resolution) ─────────────────────────────

/// Bind raw FuncArgs to the signature's argument slots.
/// Returns a vector of `Option<Box<Ast>>` aligned with the signature.
fn bind_args(
    sig: &Signature,
    raw: &[FuncArg],
    fn_name: &str,
    _scope: &BindingInfo,
    _semana: &SemanticAnalysis,
) -> Result<Vec<Option<Box<Ast>>>, String> {
    let mut result: Vec<Option<Box<Ast>>> = vec![None; sig.args.len()];
    let mut had_named = false;
    let mut pos = 0;

    for fa in raw {
        let (fa_name, fa_value_ast) = match fa {
            FuncArg::Flat { name, value: _ } => (name.as_deref(), flatten_func_arg_value(fa)),
            FuncArg::List { name, items: _ } => (name.as_deref(), flatten_func_arg_list(fa)),
        };

        if let Some(n) = fa_name {
            if !had_named {
                had_named = true;
            }
            let slot = sig
                .args
                .iter()
                .position(|a| a.name == n)
                .ok_or_else(|| format!("parameter '{n}' not found in call to '{fn_name}'"))?;
            if result[slot].is_some() {
                return Err(format!(
                    "parameter '{n}' provided more than once in call to '{fn_name}'"
                ));
            }
            result[slot] = Some(fa_value_ast);
        } else {
            if had_named {
                return Err(format!("positional parameters cannot be used after named parameters in call to '{fn_name}'"));
            }
            if pos >= sig.args.len() {
                return Err(format!("too many parameters in call to '{fn_name}'"));
            }
            result[pos] = Some(fa_value_ast);
            pos += 1;
        }
    }

    // Check required arguments
    for (i, a) in sig.args.iter().enumerate() {
        if result[i].is_none() && !a.has_default {
            return Err(format!(
                "parameter '{}' missing in call to '{fn_name}'",
                a.name
            ));
        }
    }

    Ok(result)
}

/// Convert a FuncArg to a synthetic AST node representing its value.
fn flatten_func_arg_value(fa: &FuncArg) -> Box<Ast> {
    match fa {
        FuncArg::Flat { value, .. } => value.clone(),
        FuncArg::List { items, .. } => {
            // Wrap items in a synthetic __exprlist__ call node
            let args: Vec<FuncArg> = items
                .iter()
                .map(|item| match item {
                    FuncArgNamed::Flat { name, value } => FuncArg::Flat {
                        name: name.clone(),
                        value: value.clone(),
                    },
                    FuncArgNamed::Case { key, value } => {
                        // Encode key => value as a synthetic __case_entry__(key, value) call
                        // so that eval_funcarg_named_list can recover both parts.
                        FuncArg::Flat {
                            name: None,
                            value: Box::new(Ast::Call {
                                func: Box::new(Ast::Identifier("__case_entry__".into())),
                                args: vec![
                                    FuncArg::Flat {
                                        name: None,
                                        value: key.clone(),
                                    },
                                    FuncArg::Flat {
                                        name: None,
                                        value: value.clone(),
                                    },
                                ],
                            }),
                        }
                    }
                    FuncArgNamed::List { name, items } => FuncArg::List {
                        name: name.clone(),
                        items: items.clone(),
                    },
                })
                .collect();
            Box::new(Ast::Call {
                func: Box::new(Ast::Identifier("__exprlist__".into())),
                args,
            })
        }
    }
}

fn flatten_func_arg_list(fa: &FuncArg) -> Box<Ast> {
    flatten_func_arg_value(fa)
}

// ── signature tables ──────────────────────────────────────────────────────────

fn free_function_sig(name: &str) -> Option<Signature> {
    use TypeCategory::*;
    let args = match name {
        "count" => vec![a("value", Expression, true), a("distinct", Symbol, true)],
        "sum" => vec![a("value", Expression, false), a("distinct", Symbol, true)],
        "avg" => vec![a("value", Expression, false), a("distinct", Symbol, true)],
        "min" => vec![a("value", Expression, false)],
        "max" => vec![a("value", Expression, false)],
        "row_number" => vec![],
        "rank" => vec![a("value", Expression, false)],
        "dense_rank" => vec![a("value", Expression, false)],
        "ntile" => vec![a("n", Expression, false)],
        "lead" => vec![
            a("value", Expression, false),
            a("offset", Expression, true),
            a("default", Expression, true),
        ],
        "lag" => vec![
            a("value", Expression, false),
            a("offset", Expression, true),
            a("default", Expression, true),
        ],
        "first_value" => vec![a("value", Expression, false)],
        "last_value" => vec![a("value", Expression, false)],
        "table" => vec![a("values", ExpressionList, false)],
        "case" => vec![
            a("cases", ExpressionList, false),
            a("else", Expression, true),
            a("search", Scalar, true),
        ],
        "gensym" => vec![a("name", Symbol, true)],
        "foreigncall" => vec![
            a("name", Scalar, false),
            a("returns", Symbol, false),
            a("arguments", ExpressionList, true),
            a("type", Symbol, true),
        ],
        _ => return None,
    };
    Some(Signature { args })
}

fn method_sig(base: &ExpressionResult, name: &str) -> Option<Signature> {
    use TypeCategory::*;
    if base.is_table() {
        let args = match name {
            "filter" => vec![a("condition", Expression, false)],
            "join" => vec![
                a("table", Table, false),
                a("on", Expression, false),
                a("type", Symbol, true),
            ],
            "groupby" => vec![
                a("groups", ExpressionList, false),
                a("aggregates", ExpressionList, true),
                a("type", Symbol, true),
                a("sets", ExpressionList, true),
            ],
            "aggregate" => vec![a("aggregate", Expression, false)],
            "distinct" => vec![],
            "orderby" => vec![
                a("expressions", ExpressionList, false),
                a("limit", Expression, true),
                a("offset", Expression, true),
            ],
            "map" => vec![a("expressions", ExpressionList, false)],
            "project" => vec![a("expressions", ExpressionList, false)],
            "projectout" => vec![a("columns", ExpressionList, false)],
            "as" => vec![a("name", Symbol, false)],
            "alias" => vec![a("name", Symbol, false)],
            "union" => vec![a("table", Table, false), a("all", Symbol, true)],
            "except" => vec![a("table", Table, false), a("all", Symbol, true)],
            "intersect" => vec![a("table", Table, false), a("all", Symbol, true)],
            "window" => vec![
                a("expressions", ExpressionList, false),
                a("partitionby", ExpressionList, true),
                a("orderby", ExpressionList, true),
                a("framebegin", Expression, true),
                a("framend", Expression, true),
                a("frametype", Symbol, true),
            ],
            _ => return None,
        };
        Some(Signature { args })
    } else {
        // Scalar methods
        let args = match name {
            "asc" => vec![],
            "desc" => vec![],
            "collate" => vec![a("collate", Symbol, false)],
            "is" => vec![a("other", Scalar, false)],
            "like" => vec![a("pattern", Scalar, false)],
            "between" => vec![a("lower", Scalar, false), a("upper", Scalar, false)],
            "in" => vec![a("values", ExpressionList, false)],
            "substr" => vec![a("from", Scalar, true), a("for", Scalar, true)],
            "extract" => vec![a("part", Symbol, false)],
            _ => return None,
        };
        Some(Signature { args })
    }
}

fn a(name: &str, category: TypeCategory, has_default: bool) -> SigArg {
    SigArg {
        name: name.to_string(),
        category,
        has_default,
    }
}

// ── utilities ─────────────────────────────────────────────────────────────────

fn parse_simple_type(name: &str) -> Result<Type, String> {
    match name {
        "integer" => Ok(Type::integer()),
        "boolean" => Ok(Type::bool_()),
        "date" => Ok(Type::date()),
        "interval" => Ok(Type::interval()),
        "text" => Ok(Type::text()),
        other => Err(format!("unknown type '{other}'")),
    }
}

fn infer_decimal_type(s: &str) -> Result<Type, String> {
    let s = s.trim_start_matches(['+', '-']);
    let dot = s.find('.');
    let before = if let Some(d) = dot { d } else { s.len() };
    let after = if let Some(d) = dot {
        s.len() - d - 1
    } else {
        0
    };
    let precision = (before + after).max(1) as u32;
    let scale = after as u32;
    if precision > 38 {
        return Err("decimal value out of range".into());
    }
    Ok(Type::decimal(precision, scale))
}

fn extract_integer_const(ast: &Ast) -> Result<u64, String> {
    match ast {
        Ast::Literal(Literal::Integer(s)) => s
            .parse()
            .map_err(|_| format!("invalid integer constant '{s}'")),
        _ => Err("expected integer constant".into()),
    }
}

fn infer_name(ast: &Ast) -> String {
    match ast {
        Ast::Identifier(n) => n.clone(),
        Ast::Access { part, .. } => part.clone(),
        _ => String::new(),
    }
}

/// Comparable type check + NULL coercion (mirrors `enforceComparable`).
fn enforce_comparable_exprs(a: &mut Box<Expr>, b: &mut Box<Expr>) -> Result<(), String> {
    let at = a.typ();
    let bt = b.typ();

    if at.base == TypeBase::Unknown && bt.base == TypeBase::Unknown {
        return Ok(());
    }
    if at.base == TypeBase::Unknown {
        let old = std::mem::replace(a, Box::new(dummy_expr()));
        *a = Box::new(Expr::Cast {
            input: old,
            typ: bt.as_nullable(),
        });
        return Ok(());
    }
    if bt.base == TypeBase::Unknown {
        let old = std::mem::replace(b, Box::new(dummy_expr()));
        *b = Box::new(Expr::Cast {
            input: old,
            typ: at.as_nullable(),
        });
        return Ok(());
    }

    let ok = match at.base {
        TypeBase::Bool => bt.base == TypeBase::Bool,
        TypeBase::Integer | TypeBase::Decimal { .. } => {
            matches!(bt.base, TypeBase::Integer | TypeBase::Decimal { .. })
        }
        TypeBase::Char { .. } | TypeBase::Varchar { .. } | TypeBase::Text => matches!(
            bt.base,
            TypeBase::Char { .. } | TypeBase::Varchar { .. } | TypeBase::Text
        ),
        TypeBase::Date => bt.base == TypeBase::Date,
        TypeBase::Interval => bt.base == TypeBase::Interval,
        TypeBase::Unknown => true,
    };
    if !ok {
        return Err(format!(
            "cannot compare '{}' and '{}'",
            at.name(),
            bt.name()
        ));
    }
    Ok(())
}

/// Take a scalar expr out of an ExpressionResult, leaving a dummy.
fn take_expr(er: &mut ExpressionResult) -> Box<Expr> {
    std::mem::replace(er.expr_mut(), Box::new(dummy_expr()))
}

fn dummy_expr() -> Expr {
    Expr::Const {
        value: Some("NULL".into()),
        typ: Type::unknown(),
    }
}

/// Insert a Map operator just below any outermost Sort.
fn insert_map_below_sort(tree: Box<Op>, computations: Vec<MapEntry>) -> Box<Op> {
    match *tree {
        Op::Sort {
            input,
            order,
            limit,
            offset,
        } => {
            let new_input = Box::new(Op::Map {
                input,
                computations,
            });
            Box::new(Op::Sort {
                input: new_input,
                order,
                limit,
                offset,
            })
        }
        other => Box::new(Op::Map {
            input: Box::new(other),
            computations,
        }),
    }
}
