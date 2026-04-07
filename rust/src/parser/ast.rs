/// Top-level AST node
#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    DefineFunction {
        name: String,
        args: Vec<LetArg>,
        body: Box<Ast>,
    },
    QueryBody {
        lets: Vec<LetEntry>,
        body: Box<Ast>,
    },

    // Expressions
    Identifier(String),
    Literal(Literal),
    Cast {
        value: Box<Ast>,
        typ: Box<Type>,
    },
    UnaryExpr {
        op: UnaryOp,
        value: Box<Ast>,
    },
    BinaryExpr {
        op: BinaryOp,
        left: Box<Ast>,
        right: Box<Ast>,
    },
    /// Member access: `base.part`
    Access {
        base: Box<Ast>,
        part: String,
    },
    /// Function/method call: `func(args)`
    Call {
        func: Box<Ast>,
        args: Vec<FuncArg>,
    },
}

/// A let-binding entry: `let name(args) := body,`
#[derive(Debug, Clone, PartialEq)]
pub struct LetEntry {
    pub name: String,
    pub args: Vec<LetArg>,
    pub body: Box<Ast>,
}

/// A single argument in a let-binding or defun signature
#[derive(Debug, Clone, PartialEq)]
pub struct LetArg {
    pub name: String,
    pub typ: Option<Type>,
    pub default: Option<Box<Ast>>,
}

/// Literal constants
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(String),
    Float(String),
    String(String),
    True,
    False,
    Null,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

/// Binary operators, in precedence order (high to low)
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    // Arithmetic
    Pow,
    Mul,
    Div,
    Mod,
    Plus,
    Minus,
    // Comparison
    Less,
    Greater,
    Equals,
    LessOrEqual,
    GreaterOrEqual,
    NotEquals,
    // Logical
    And,
    Or,
}

/// Type reference
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// `name`
    Simple(String),
    /// `name{type, type, ...}` — e.g. `decimal{10, 2}`
    SubTypes { name: String, args: Vec<Type> },
    /// `name{key := val, ...}` — e.g. `varchar{length := 20}`
    Parameter { name: String, args: Vec<TypeArg> },
}

/// A named or positional type argument: `key := value` or just `value`
#[derive(Debug, Clone, PartialEq)]
pub struct TypeArg {
    pub name: Option<String>,
    pub value: String, // always an integer literal
}

/// A single argument at a call site: `name := expr`, `name := {list}`,
/// positional `expr`, or positional `{list}`
#[derive(Debug, Clone, PartialEq)]
pub enum FuncArg {
    /// Positional or named scalar: `expr` or `name := expr`
    Flat {
        name: Option<String>,
        value: Box<Ast>,
    },
    /// Positional or named list: `{items}` or `name := {items}`
    List {
        name: Option<String>,
        items: Vec<FuncArgNamed>,
    },
}

/// An element inside a `{...}` argument list
#[derive(Debug, Clone, PartialEq)]
pub enum FuncArgNamed {
    /// Positional or named scalar: `expr` or `name := expr`
    Flat {
        name: Option<String>,
        value: Box<Ast>,
    },
    /// Case-mapping entry: `key => value`
    Case {
        key: Box<Ast>,
        value: Box<Ast>,
    },
    /// Nested list: `{items}` or `name := {items}`
    List {
        name: Option<String>,
        items: Vec<FuncArgNamed>,
    },
}
