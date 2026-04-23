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
//! const sql = await compile(query, async (tableName) => {
//!     const cols = await myMetastore.columns(tableName);
//!     if (!cols) return null;
//!     return cols.map(c => ({ name: c.name, type: c.trinoType }));
//! });
//! ```
//!
//! The `get_columns` callback may be **async** (return a `Promise`).
//! The outer `compile` call is also async (returns a `Promise<string>`).

#[cfg(feature = "wasm")]
mod inner {
    use js_sys::{Array, Function, JsString, Object, Promise, Reflect};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    macro_rules! log {
        ($($t:tt)*) => {
            web_sys::console::log_1(&format!($($t)*).into())
        };
    }

    use std::future::Future;
    use std::pin::Pin;

    use crate::infra::schema::{Column, SchemaProvider, Table, Type, parse_type_str};
    use crate::compile_inner;

    /// Compile a SaneQL *query* string to SQL.
    ///
    /// * `query`       — the SaneQL source text.
    /// * `get_columns` — an **async** JS function
    ///   `(tableName: string) => Promise<Array<{name: string, type: string}> | null>`.
    ///   Called lazily for each table referenced in the query.
    ///   Return `null` (or `undefined`) if the table does not exist.
    fn js_err_to_string(table: &str, e: JsValue) -> String {
        let msg = js_sys::Error::from(e)
            .message()
            .as_string()
            .unwrap_or_else(|| "unknown error".into());
        format!("error retrieving schema for table '{}': {}", table, msg)
    }

    #[wasm_bindgen]
    pub async fn compile(query: &str, get_columns: Function) -> Result<String, JsError> {
        struct JsProvider(Function);

        impl SchemaProvider for JsProvider {
            fn lookup_table<'a>(
                &'a self,
                name: &'a str,
            ) -> Pin<Box<dyn Future<Output = Result<Option<Table>, String>> + 'a>> {
                Box::pin(async move {
                    log!("[saneql] looking up table: {}", name);
                    let promise = self
                        .0
                        .call1(&JsValue::NULL, &JsValue::from_str(name))
                        .map_err(|e| js_err_to_string(name, e))?;
                    let result = JsFuture::from(Promise::from(promise))
                        .await
                        .map_err(|e| js_err_to_string(name, e))?;

                    // null / undefined → table does not exist
                    if result.is_null() || result.is_undefined() {
                        return Err(format!("table '{}' not found", name));
                    }

                    // Expect an Array of {name, type} objects.
                    let arr = Array::from(&result);
                    let mut columns = Vec::new();
                    for item in arr.iter() {
                        let obj: &Object = item.dyn_ref::<Object>()
                            .ok_or_else(|| format!("column descriptor for table '{}' is not an object", name))?;
                        let name_val = Reflect::get(obj, &JsString::from("name"))
                            .map_err(|e| format!("missing 'name' field in column descriptor for table '{}': {:?}", name, e))?;
                        let type_val = Reflect::get(obj, &JsString::from("type"))
                            .map_err(|e| format!("missing 'type' field in column descriptor for table '{}': {:?}", name, e))?;
                        let col_name = name_val.as_string()
                            .ok_or_else(|| format!("'name' field is not a string in column descriptor for table '{}'", name))?;
                        let type_str = type_val.as_string()
                            .ok_or_else(|| format!("'type' field is not a string in column descriptor for table '{}'", name))?;
                        let typ = parse_type_str(&type_str).unwrap_or_else(Type::unknown);
                        columns.push(Column { name: col_name, typ });
                    }
                    log!("[saneql] resolved table '{}': {} column(s)", name, columns.len());
                    Ok(Some(Table { columns }))
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
