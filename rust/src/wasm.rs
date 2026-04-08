//! WebAssembly bindings for SaneQL.
//!
//! Compiled with `--features wasm` (i.e. `wasm-pack build --features wasm`).
//!
//! JS API
//! ------
//! ```js
//! import init, { compile } from './pkg/saneql.js';
//! await init();
//!
//! const sql = await compile(query, (tableName) => {
//!     const cols = myMetastore.columnsSync(tableName);
//!     if (!cols) return null;
//!     return cols.map(c => ({ name: c.name, type: c.trinoType }));
//! });
//! ```
//!
//! The `get_columns` callback must be **synchronous**.  The outer `compile`
//! call is already async (returns a `Promise<string>`), so the caller awaits
//! that.  If schema metadata needs to be fetched asynchronously, pre-fetch
//! and cache it before calling `compile`.

#[cfg(feature = "wasm")]
mod inner {
    use js_sys::{Array, Function, JsString, Object, Reflect};
    use wasm_bindgen::prelude::*;

    use std::future::Future;
    use std::pin::Pin;

    use crate::infra::schema::{Column, SchemaProvider, Table, Type, parse_type_str};
    use crate::compile_inner;

    /// Compile a SaneQL *query* string to SQL.
    ///
    /// * `query`       — the SaneQL source text.
    /// * `get_columns` — a **sync** JS function
    ///   `(tableName: string) => Array<{name: string, type: string}> | null`.
    ///   Called lazily for each table referenced in the query.
    ///   Return `null` (or `undefined`) if the table does not exist.
    #[wasm_bindgen]
    pub async fn compile(query: &str, get_columns: Function) -> Result<String, JsError> {
        struct JsProvider(Function);

        impl SchemaProvider for JsProvider {
            fn lookup_table<'a>(
                &'a self,
                name: &'a str,
            ) -> Pin<Box<dyn Future<Output = Option<Table>> + 'a>> {
                Box::pin(async move {
                    let result = self
                        .0
                        .call1(&JsValue::NULL, &JsValue::from_str(name))
                        .ok()?;

                    // null / undefined → table not found
                    if result.is_null() || result.is_undefined() {
                        return None;
                    }

                    // Expect an Array of {name, type} objects.
                    let arr = Array::from(&result);
                    let mut columns = Vec::new();
                    for item in arr.iter() {
                        let obj: &Object = item.dyn_ref::<Object>()?;
                        let name_val = Reflect::get(obj, &JsString::from("name")).ok()?;
                        let type_val = Reflect::get(obj, &JsString::from("type")).ok()?;
                        let col_name = name_val.as_string()?;
                        let type_str = type_val.as_string()?;
                        let typ = parse_type_str(&type_str).unwrap_or_else(Type::unknown);
                        columns.push(Column { name: col_name, typ });
                    }
                    Some(Table { columns })
                })
            }
        }

        compile_inner(query, Box::new(JsProvider(get_columns)))
            .await
            .map_err(|e| JsError::new(&e))
    }
}

#[cfg(feature = "wasm")]
pub use inner::compile;
