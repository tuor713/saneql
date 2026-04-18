/// TPC-H schema definition used by the golden tests.
///
/// Each entry is `(table_name, &[(column_name, type_str)])`.
/// Type strings follow the Trino / SQL convention understood by
/// `saneql::infra::schema::parse_type_str`.
pub const TPCH_TABLES: &[(&str, &[(&str, &str)])] = &[
    (
        "part",
        &[
            ("p_partkey", "integer"),
            ("p_name", "varchar(55)"),
            ("p_mfgr", "char(25)"),
            ("p_brand", "char(10)"),
            ("p_type", "varchar(25)"),
            ("p_size", "integer"),
            ("p_container", "char(10)"),
            ("p_retailprice", "decimal(12,2)"),
            ("p_comment", "varchar(23)"),
        ],
    ),
    (
        "region",
        &[
            ("r_regionkey", "integer"),
            ("r_name", "char(25)"),
            ("r_comment", "varchar(152)"),
        ],
    ),
    (
        "nation",
        &[
            ("n_nationkey", "integer"),
            ("n_name", "char(25)"),
            ("n_regionkey", "integer"),
            ("n_comment", "varchar(152)"),
        ],
    ),
    (
        "supplier",
        &[
            ("s_suppkey", "integer"),
            ("s_name", "char(25)"),
            ("s_address", "varchar(40)"),
            ("s_nationkey", "integer"),
            ("s_phone", "char(15)"),
            ("s_acctbal", "decimal(12,2)"),
            ("s_comment", "varchar(101)"),
        ],
    ),
    (
        "partsupp",
        &[
            ("ps_partkey", "integer"),
            ("ps_suppkey", "integer"),
            ("ps_availqty", "integer"),
            ("ps_supplycost", "decimal(12,2)"),
            ("ps_comment", "varchar(199)"),
        ],
    ),
    (
        "customer",
        &[
            ("c_custkey", "integer"),
            ("c_name", "varchar(25)"),
            ("c_address", "varchar(40)"),
            ("c_nationkey", "integer"),
            ("c_phone", "char(15)"),
            ("c_acctbal", "decimal(12,2)"),
            ("c_mktsegment", "char(10)"),
            ("c_comment", "varchar(117)"),
        ],
    ),
    (
        "orders",
        &[
            ("o_orderkey", "integer"),
            ("o_custkey", "integer"),
            ("o_orderstatus", "char(1)"),
            ("o_totalprice", "decimal(12,2)"),
            ("o_orderdate", "date"),
            ("o_orderpriority", "char(15)"),
            ("o_clerk", "char(15)"),
            ("o_shippriority", "integer"),
            ("o_comment", "varchar(79)"),
        ],
    ),
    (
        "lineitem",
        &[
            ("l_orderkey", "integer"),
            ("l_partkey", "integer"),
            ("l_suppkey", "integer"),
            ("l_linenumber", "integer"),
            ("l_quantity", "decimal(12,2)"),
            ("l_extendedprice", "decimal(12,2)"),
            ("l_discount", "decimal(12,2)"),
            ("l_tax", "decimal(12,2)"),
            ("l_returnflag", "char(1)"),
            ("l_linestatus", "char(1)"),
            ("l_shipdate", "date"),
            ("l_commitdate", "date"),
            ("l_receiptdate", "date"),
            ("l_shipinstruct", "char(25)"),
            ("l_shipmode", "char(10)"),
            ("l_comment", "varchar(44)"),
        ],
    ),
];

/// Return a schema-lookup closure over the TPC-H tables suitable for passing
/// to [`saneql::compile_with_schema`].
pub fn tpch_schema() -> impl Fn(&str) -> Option<Vec<(String, String)>> {
    |table_name: &str| {
        TPCH_TABLES.iter().find(|(name, _)| *name == table_name).map(|(_, cols)| {
            cols.iter()
                .map(|(col, typ)| (col.to_string(), typ.to_string()))
                .collect()
        })
    }
}
