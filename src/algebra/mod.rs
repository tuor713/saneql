use crate::infra::schema::Type;
use crate::sql::writer::SqlWriter;
use std::rc::Rc;

// ── IU (Information Unit) ────────────────────────────────────────────────────

/// Represents a column/intermediate value with a type and a unique identity.
/// Shared via `Rc`; identity is tracked by `id`.
#[derive(Debug, PartialEq, Eq)]
pub struct IU {
    pub id: u64,
    pub typ: Type,
}

impl IU {
    pub fn new(id: u64, typ: Type) -> Rc<Self> {
        Rc::new(IU { id, typ })
    }
}

// ── Aggregation operation kinds ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    // Shared between GroupBy and Window:
    CountStar,
    Count,
    CountDistinct,
    Sum,
    SumDistinct,
    Min,
    Max,
    Avg,
    AvgDistinct,
    // Window-only:
    RowNumber,
    Rank,
    DenseRank,
    NTile,
    Lead,
    Lag,
    FirstValue,
    LastValue,
}

/// An aggregation step inside GroupBy / Window / Aggregate.
#[derive(Debug)]
pub struct Aggregation {
    pub value: Option<Box<Expr>>, // None for CountStar
    pub iu: Rc<IU>,
    pub op: AggOp,
    pub params: Vec<Box<Expr>>, // lead/lag offset + default
}

/// A computed column inside Map / GroupBy result projection.
#[derive(Debug)]
pub struct MapEntry {
    pub value: Box<Expr>,
    pub iu: Option<Rc<IU>>, // None means "pass-through IURef" (no new computation needed)
}

// ── Sort ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Collate; // only None for now

#[derive(Debug)]
pub struct SortEntry {
    pub value: Box<Expr>,
    pub collate: Collate,
    pub descending: bool,
}

// ── Join / SetOp kinds ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    UnionAll,
    Except,
    ExceptAll,
    Intersect,
    IntersectAll,
}

// ── ForeignCall type ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallType {
    Function,
    LeftAssoc,
    RightAssoc,
    /// SQL keyword — rendered as bare name, no parentheses (e.g. `current_date`)
    Keyword,
}

// ── Extract date part ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
}

// ── Comparison mode ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpMode {
    Equal,
    NotEqual,
    Is,
    IsNot,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Like,
}

// ── Binary / Unary ops ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    Power,
    Concat,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Plus,
    Minus,
    Not,
}

// ── Expr ─────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Expr {
    IURef(Rc<IU>),
    Const {
        value: Option<String>,
        typ: Type,
    }, // None = NULL
    Cast {
        input: Box<Expr>,
        typ: Type,
    },
    Comparison {
        left: Box<Expr>,
        right: Box<Expr>,
        mode: CmpMode,
        collate: Collate,
    },
    Between {
        base: Box<Expr>,
        lower: Box<Expr>,
        upper: Box<Expr>,
        collate: Collate,
    },
    In {
        probe: Box<Expr>,
        values: Vec<Box<Expr>>,
        collate: Collate,
    },
    Binary {
        left: Box<Expr>,
        right: Box<Expr>,
        typ: Type,
        op: BinOp,
    },
    Unary {
        input: Box<Expr>,
        typ: Type,
        op: UnOp,
    },
    Extract {
        input: Box<Expr>,
        part: DatePart,
    },
    Substr {
        value: Box<Expr>,
        from: Option<Box<Expr>>,
        len: Option<Box<Expr>>,
    },
    SimpleCase {
        value: Box<Expr>,
        cases: Vec<(Box<Expr>, Box<Expr>)>,
        default: Box<Expr>,
    },
    SearchedCase {
        cases: Vec<(Box<Expr>, Box<Expr>)>,
        default: Box<Expr>,
    },
    /// Scalar sub-aggregate: `(select agg from (select ... from input s) s)`
    Aggregate {
        input: Box<Op>,
        aggregates: Vec<Aggregation>,
        computation: Box<Expr>,
    },
    ForeignCall {
        name: String,
        typ: Type,
        args: Vec<Box<Expr>>,
        call_type: CallType,
    },
}

impl Expr {
    pub fn typ(&self) -> Type {
        match self {
            Expr::IURef(iu) => iu.typ,
            Expr::Const { typ, .. } => *typ,
            Expr::Cast { typ, .. } => *typ,
            Expr::Comparison { .. } => Type::bool_(), // approx
            Expr::Between { .. } => Type::bool_(),
            Expr::In { .. } => Type::bool_(),
            Expr::Binary { typ, .. } => *typ,
            Expr::Unary { typ, .. } => *typ,
            Expr::Extract { .. } => Type::integer(),
            Expr::Substr { value, .. } => value.typ(),
            Expr::SimpleCase { default, .. } => default.typ(),
            Expr::SearchedCase { default, .. } => default.typ(),
            Expr::Aggregate { computation, .. } => computation.typ(),
            Expr::ForeignCall { typ, .. } => *typ,
        }
    }

    pub fn generate(&self, out: &mut SqlWriter) {
        match self {
            Expr::IURef(iu) => out.write_iu(iu),

            Expr::Const { value: None, .. } => out.write("NULL"),
            Expr::Const {
                value: Some(v),
                typ,
            } => {
                if typ.is_string() {
                    out.write_string(v);
                } else {
                    out.write("cast(");
                    out.write_string(v);
                    out.write(" as ");
                    out.write_type(*typ);
                    out.write(")");
                }
            }

            Expr::Cast { input, typ } => {
                out.write("cast(");
                input.generate(out);
                out.write(" as ");
                out.write_type(*typ);
                out.write(")");
            }

            Expr::Comparison {
                left, right, mode, ..
            } => {
                left.generate_operand(out);
                let op = match mode {
                    CmpMode::Equal => " = ",
                    CmpMode::NotEqual => " <> ",
                    CmpMode::Is => " is not distinct from ",
                    CmpMode::IsNot => " is distinct from ",
                    CmpMode::Less => " < ",
                    CmpMode::LessOrEqual => " <= ",
                    CmpMode::Greater => " > ",
                    CmpMode::GreaterOrEqual => " >= ",
                    CmpMode::Like => " like ",
                };
                out.write(op);
                right.generate_operand(out);
            }

            Expr::Between {
                base, lower, upper, ..
            } => {
                base.generate_operand(out);
                out.write(" between ");
                lower.generate_operand(out);
                out.write(" and ");
                upper.generate_operand(out);
            }

            Expr::In { probe, values, .. } => {
                probe.generate_operand(out);
                out.write(" in (");
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        out.write(", ");
                    }
                    v.generate(out);
                }
                out.write(")");
            }

            Expr::Binary {
                left, right, op, ..
            } => {
                left.generate_operand(out);
                let s = match op {
                    BinOp::Plus => " + ",
                    BinOp::Minus => " - ",
                    BinOp::Mul => " * ",
                    BinOp::Div => " / ",
                    BinOp::Mod => " % ",
                    BinOp::Power => " ^ ",
                    BinOp::Concat => " || ",
                    BinOp::And => " and ",
                    BinOp::Or => " or ",
                };
                out.write(s);
                right.generate_operand(out);
            }

            Expr::Unary { input, op, .. } => {
                let prefix = match op {
                    UnOp::Plus => "+",
                    UnOp::Minus => "-",
                    UnOp::Not => " not ",
                };
                out.write(prefix);
                input.generate_operand(out);
            }

            Expr::Extract { input, part } => {
                out.write("extract(");
                let p = match part {
                    DatePart::Year => "year",
                    DatePart::Month => "month",
                    DatePart::Day => "day",
                };
                out.write(p);
                out.write(" from ");
                input.generate_operand(out);
                out.write(")");
            }

            Expr::Substr { value, from, len } => {
                out.write("substring(");
                value.generate(out);
                if let Some(f) = from {
                    out.write(" from ");
                    f.generate(out);
                }
                if let Some(l) = len {
                    out.write(" for ");
                    l.generate(out);
                }
                out.write(")");
            }

            Expr::SimpleCase {
                value,
                cases,
                default,
            } => {
                out.write("case ");
                value.generate_operand(out);
                for (k, v) in cases {
                    out.write(" when ");
                    k.generate(out);
                    out.write(" then ");
                    v.generate(out);
                }
                out.write(" else ");
                default.generate(out);
                out.write(" end");
            }

            Expr::SearchedCase { cases, default } => {
                out.write("case");
                for (k, v) in cases {
                    out.write(" when ");
                    k.generate(out);
                    out.write(" then ");
                    v.generate(out);
                }
                out.write(" else ");
                default.generate(out);
                out.write(" end");
            }

            Expr::Aggregate {
                input,
                aggregates,
                computation,
            } => {
                out.write("(select ");
                computation.generate(out);
                if !aggregates.is_empty() {
                    out.write(" from (select ");
                    for (i, a) in aggregates.iter().enumerate() {
                        if i > 0 {
                            out.write(", ");
                        }
                        write_agg(out, a);
                        out.write(" as ");
                        out.write_iu(&a.iu);
                    }
                    out.write(" from ");
                    input.generate(out);
                    out.write(" s) s");
                }
                out.write(")");
            }

            Expr::ForeignCall {
                name,
                args,
                call_type,
                ..
            } => {
                match call_type {
                    CallType::Function => {
                        out.write(name);
                        out.write("(");
                        for (i, a) in args.iter().enumerate() {
                            if i > 0 {
                                out.write(", ");
                            }
                            a.generate(out);
                        }
                        out.write(")");
                    }
                    CallType::LeftAssoc => {
                        // ((a op b) op c) op d
                        for i in 0..args.len().saturating_sub(2) {
                            let _ = i;
                            out.write("(");
                        }
                        args[0].generate_operand(out);
                        for i in 1..args.len() {
                            out.write(" ");
                            out.write(name);
                            out.write(" ");
                            args[i].generate_operand(out);
                            if i != args.len() - 1 {
                                out.write(")");
                            }
                        }
                    }
                    CallType::RightAssoc => {
                        // a op (b op (c op d))
                        for (i, a) in args.iter().enumerate() {
                            a.generate_operand(out);
                            if i != args.len() - 1 {
                                out.write(" ");
                                out.write(name);
                                out.write(" (");
                            }
                        }
                        for _ in 0..args.len().saturating_sub(2) {
                            out.write(")");
                        }
                    }
                    CallType::Keyword => {
                        out.write(name);
                    }
                }
            }
        }
    }

    /// Generate wrapped in parens (for use as an operand).
    pub fn generate_operand(&self, out: &mut SqlWriter) {
        match self {
            // These are already "atomic" and don't need extra parens
            Expr::IURef(_) | Expr::Const { .. } => self.generate(out),
            _ => {
                out.write("(");
                self.generate(out);
                out.write(")");
            }
        }
    }
}

// ── Op ───────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Op {
    TableScan {
        /// Name components, e.g. `["kafka", "default", "my.topic.name"]`.
        /// Kept as separate parts so each can be quoted individually in output.
        parts: Vec<String>,
        columns: Vec<(String, Rc<IU>)>, // (col_name, iu)
    },
    Select {
        input: Box<Op>,
        condition: Box<Expr>,
    },
    Map {
        input: Box<Op>,
        computations: Vec<MapEntry>,
    },
    Join {
        left: Box<Op>,
        right: Box<Op>,
        condition: Box<Expr>,
        join_type: JoinType,
    },
    GroupBy {
        input: Box<Op>,
        group_by: Vec<MapEntry>, // (expr, iu) — iu is Some for new IUs
        aggregates: Vec<Aggregation>,
    },
    Sort {
        input: Box<Op>,
        order: Vec<SortEntry>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
    Window {
        input: Box<Op>,
        aggregates: Vec<Aggregation>,
        partition_by: Vec<Box<Expr>>,
        order_by: Vec<SortEntry>,
    },
    SetOperation {
        left: Box<Op>,
        right: Box<Op>,
        left_cols: Vec<Box<Expr>>,
        right_cols: Vec<Box<Expr>>,
        result_cols: Vec<Rc<IU>>,
        op: SetOp,
    },
    InlineTable {
        columns: Vec<Rc<IU>>,
        values: Vec<Box<Expr>>,
        row_count: usize,
    },
}

impl Op {
    pub fn generate(&self, out: &mut SqlWriter) {
        match self {
            Op::TableScan { parts, columns } => {
                out.write("(select ");
                for (i, (col, iu)) in columns.iter().enumerate() {
                    if i > 0 {
                        out.write(", ");
                    }
                    out.write_identifier(col);
                    out.write(" as ");
                    out.write_iu(iu);
                }
                out.write(" from ");
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 { out.write("."); }
                    out.write_identifier(part);
                }
                out.write(")");
            }

            Op::Select { input, condition } => {
                out.write("(select * from ");
                input.generate(out);
                out.write(" s where ");
                condition.generate(out);
                out.write(")");
            }

            Op::Map {
                input,
                computations,
            } => {
                out.write("(select *");
                println!("Output computations {computations:?}");
                for c in computations {
                    out.write(", ");
                    c.value.generate(out);
                    out.write(" as ");
                    out.write_iu(c.iu.as_ref().expect("map entry must have iu"));
                }
                out.write(" from ");
                input.generate(out);
                out.write(" s)");
            }

            Op::Join {
                left,
                right,
                condition,
                join_type,
            } => {
                let (kw, semi_anti) = match join_type {
                    JoinType::Inner => ("inner join", None),
                    JoinType::LeftOuter => ("left outer join", None),
                    JoinType::RightOuter => ("right outer join", None),
                    JoinType::FullOuter => ("full outer join", None),
                    JoinType::LeftSemi => ("", Some(("l", "r", "exists"))),
                    JoinType::RightSemi => ("", Some(("r", "l", "exists"))),
                    JoinType::LeftAnti => ("", Some(("l", "r", "not exists"))),
                    JoinType::RightAnti => ("", Some(("r", "l", "not exists"))),
                };
                if let Some((outer_alias, inner_alias, exist_kw)) = semi_anti {
                    let (outer, inner) = if outer_alias == "l" {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    out.write("(select * from ");
                    outer.generate(out);
                    out.write(&format!(" {outer_alias} where {exist_kw}(select * from "));
                    inner.generate(out);
                    out.write(&format!(" {inner_alias} where "));
                    condition.generate(out);
                    out.write("))");
                } else {
                    out.write("(select * from ");
                    left.generate(out);
                    out.write(&format!(" l {kw} "));
                    right.generate(out);
                    out.write(" r on ");
                    condition.generate(out);
                    out.write(")");
                }
            }

            Op::GroupBy {
                input,
                group_by,
                aggregates,
            } => {
                out.write("(select ");
                let mut first = true;
                for g in group_by {
                    if !first {
                        out.write(", ");
                    }
                    first = false;
                    g.value.generate(out);
                    out.write(" as ");
                    out.write_iu(g.iu.as_ref().expect("groupby iu"));
                }
                for a in aggregates {
                    if !first {
                        out.write(", ");
                    }
                    first = false;
                    write_agg(out, a);
                    out.write(" as ");
                    out.write_iu(&a.iu);
                }
                out.write(" from ");
                input.generate(out);
                out.write(" s group by ");
                if group_by.is_empty() {
                    out.write("true");
                } else {
                    for i in 0..group_by.len() {
                        if i > 0 {
                            out.write(", ");
                        }
                        out.write(&(i + 1).to_string());
                    }
                }
                out.write(")");
            }

            Op::Sort {
                input,
                order,
                limit,
                offset,
            } => {
                out.write("(select * from ");
                input.generate(out);
                out.write(" s");
                if !order.is_empty() {
                    out.write(" order by ");
                    for (i, o) in order.iter().enumerate() {
                        if i > 0 {
                            out.write(", ");
                        }
                        o.value.generate(out);
                        if o.descending {
                            out.write(" desc");
                        }
                    }
                }
                if let Some(l) = limit {
                    out.write(" limit ");
                    out.write(&l.to_string());
                }
                if let Some(o) = offset {
                    out.write(" offset ");
                    out.write(&o.to_string());
                }
                out.write(")");
            }

            Op::Window {
                input,
                aggregates,
                partition_by,
                order_by,
            } => {
                // Wrap the window sub-query: (select * from (select *, AGG over (…) …) s)
                out.write("(select * from (select *");
                for a in aggregates {
                    out.write(", ");
                    write_window_agg(out, a);
                    out.write(" over (");
                    if !partition_by.is_empty() {
                        out.write("partition by ");
                        for (i, p) in partition_by.iter().enumerate() {
                            if i > 0 {
                                out.write(", ");
                            }
                            p.generate(out);
                        }
                    }
                    if !order_by.is_empty() {
                        if !partition_by.is_empty() {
                            out.write(" ");
                        }
                        out.write("order by ");
                        for (i, o) in order_by.iter().enumerate() {
                            if i > 0 {
                                out.write(", ");
                            }
                            o.value.generate(out);
                            if o.descending {
                                out.write(" desc");
                            }
                        }
                    }
                    out.write(") as ");
                    out.write_iu(&a.iu);
                }
                out.write(" from ");
                input.generate(out);
                out.write(" s) s)");
            }

            Op::SetOperation {
                left,
                right,
                left_cols,
                right_cols,
                result_cols,
                op,
            } => {
                let kw = match op {
                    SetOp::Union => "union",
                    SetOp::UnionAll => "union all",
                    SetOp::Except => "except",
                    SetOp::ExceptAll => "except all",
                    SetOp::Intersect => "intersect",
                    SetOp::IntersectAll => "intersect all",
                };
                let dump = |cols: &[Box<Expr>], out: &mut SqlWriter| {
                    if cols.is_empty() {
                        out.write("1");
                    } else {
                        for (i, c) in cols.iter().enumerate() {
                            if i > 0 {
                                out.write(", ");
                            }
                            c.generate(out);
                        }
                    }
                };
                out.write("(select * from ((select ");
                dump(left_cols, out);
                out.write(" from ");
                left.generate(out);
                out.write(&format!(" l) {kw} (select "));
                dump(right_cols, out);
                out.write(" from ");
                right.generate(out);
                out.write(" r)) s");
                if !result_cols.is_empty() {
                    out.write("(");
                    for (i, iu) in result_cols.iter().enumerate() {
                        if i > 0 {
                            out.write(", ");
                        }
                        out.write_iu(iu);
                    }
                    out.write(")");
                }
                out.write(")");
            }

            Op::InlineTable {
                columns,
                values,
                row_count,
            } => {
                out.write("(select * from (values");
                if *row_count > 0 {
                    let ncols = columns.len();
                    for row in 0..*row_count {
                        if row > 0 {
                            out.write(",");
                        }
                        if ncols > 0 {
                            out.write("(");
                            for col in 0..ncols {
                                if col > 0 {
                                    out.write(", ");
                                }
                                values[row * ncols + col].generate(out);
                            }
                            out.write(")");
                        } else {
                            out.write("(NULL)");
                        }
                    }
                } else {
                    // empty table
                    if !columns.is_empty() {
                        out.write("(");
                        for (i, _) in columns.iter().enumerate() {
                            if i > 0 {
                                out.write(", ");
                            }
                            out.write("NULL");
                        }
                        out.write(")");
                    } else {
                        out.write("(NULL)");
                    }
                }
                out.write(") s(");
                for (i, iu) in columns.iter().enumerate() {
                    if i > 0 {
                        out.write(", ");
                    }
                    out.write_iu(iu);
                }
                out.write(")");
                if *row_count == 0 {
                    out.write(" limit 0");
                }
                out.write(")");
            }
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn write_agg(out: &mut SqlWriter, a: &Aggregation) {
    match a.op {
        AggOp::CountStar => {
            out.write("count(*)");
            return;
        }
        AggOp::Count => out.write("count("),
        AggOp::CountDistinct => out.write("count(distinct "),
        AggOp::Sum => out.write("sum("),
        AggOp::SumDistinct => out.write("sum(distinct "),
        AggOp::Avg => out.write("avg("),
        AggOp::AvgDistinct => out.write("avg(distinct "),
        AggOp::Min => out.write("min("),
        AggOp::Max => out.write("max("),
        _ => unreachable!("window op in GroupBy"),
    }
    if let Some(v) = &a.value {
        v.generate(out);
    }
    out.write(")");
}

fn write_window_agg(out: &mut SqlWriter, a: &Aggregation) {
    match a.op {
        AggOp::CountStar => {
            out.write("count(*)");
            return;
        }
        AggOp::Count => out.write("count("),
        AggOp::CountDistinct => out.write("count(distinct "),
        AggOp::Sum => out.write("sum("),
        AggOp::SumDistinct => out.write("sum(distinct "),
        AggOp::Avg => out.write("avg("),
        AggOp::AvgDistinct => out.write("avg(distinct "),
        AggOp::Min => out.write("min("),
        AggOp::Max => out.write("max("),
        AggOp::RowNumber => {
            out.write("row_number()");
            return;
        }
        AggOp::Rank => out.write("rank("),
        AggOp::DenseRank => out.write("dense_rank("),
        AggOp::NTile => out.write("ntile("),
        AggOp::Lead => out.write("lead("),
        AggOp::Lag => out.write("lag("),
        AggOp::FirstValue => out.write("first_value("),
        AggOp::LastValue => out.write("last_value("),
    }
    if let Some(v) = &a.value {
        v.generate(out);
    }
    for p in &a.params {
        out.write(", ");
        p.generate(out);
    }
    out.write(")");
}
