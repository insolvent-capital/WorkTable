use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

/// Generates type alias for new [`WorkTable`].
///
/// [`WorkTable`]: worktable::WorkTable
pub fn gen_table_def(mut name: String, pk_type: String, row_type: String) -> TokenStream {
    name.push_str("WorkTable");
    let ident = Ident::new(name.as_str(), Span::mixed_site());
    let pk_type = Ident::new(pk_type.as_str(), Span::call_site());
    let row_type = Ident::new(row_type.as_str(), Span::call_site());
    return quote! {
        type #ident<I> = worktable::WorkTable<#row_type, #pk_type, I>;
    };
}

#[cfg(test)]
mod tests {
    use super::gen_table_def;

    #[test]
    fn generates_name() {
        let tokens = gen_table_def("Test".to_string(), "i64".to_string(),"TestRow".to_string());
        assert_eq!(
            tokens.to_string(),
            "type TestWorkTable < I > = worktable :: WorkTable < TestRow , i64 , I > ;"
        )
    }
}
