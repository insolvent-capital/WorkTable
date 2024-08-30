use crate::worktable::parse_columns::Columns;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

pub fn gen_row_def(columns: Columns, mut name: String) -> (TokenStream, Ident) {
    let ident = Ident::new(format!("{name}Row").as_str(), Span::mixed_site());
    let struct_def = quote! {pub struct #ident};

    let pk_ident = columns.primary_key;
    let pk_type = columns
        .columns_map
        .get(&pk_ident)
        .expect("exist because ident exist");

    let row_impl = quote! {
        impl TableRow<#pk_type> for #ident {
            const ROW_SIZE: usize = ::core::mem::size_of::<#ident>();

            fn get_primary_key(&self) -> &#pk_type {
                &self.#pk_ident
            }
        }
    };

    let rows: Vec<_> = columns
        .columns_map
        .into_iter()
        .map(|(name, type_)| {
            quote! {#name: #type_,}
        })
        .collect();

    (
        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
            #[archive(compare(PartialEq))]
            #[archive_attr(derive(Debug))]
            #[repr(C)]
            #struct_def {
                #(#rows)*
            }

            #row_impl
        },
        ident,
    )
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use crate::worktable::parse_columns::parse_columns;

    use super::gen_row_def;

    #[test]
    fn test_row_generation() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64,
        }});

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens).unwrap();
        let (row_def, row_name) = gen_row_def(columns, "Test".to_string());

        assert_eq!(row_name.to_string(), "TestRow");
    }
}
