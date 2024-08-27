use proc_macro::TokenStream;

mod performance_measurement;

#[proc_macro_attribute]
pub fn performance_measurement(attr: TokenStream, item: TokenStream) -> TokenStream {
    performance_measurement::expand(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
