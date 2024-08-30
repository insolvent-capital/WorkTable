use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::Index;
use crate::worktable::parse_columns::Columns;

pub fn gen_index_def(columns: Columns, name: &String, row_name: &Ident) -> (TokenStream, Ident) {
    let (type_def, name) = gen_type_def(columns.clone(), name);
    let impl_def = gen_impl_def(columns, &name, row_name);

    (quote! {
        #type_def

        #impl_def
    }, name)
}

pub fn gen_type_def(columns: Columns, name: &String) -> (TokenStream, Ident) {
    let index_rows = columns.indexes
        .into_iter()
        .map(|i| (
            get_index_field_name(&i),
            columns.columns_map.get(&i).clone(),
        ))
        .map(|(i, t)| {
            quote! {#i: concurrent_map::ConcurrentMap<#t, Link>}
        })
        .collect::<Vec<_>>();

    let ident = Ident::new(format!("{name}Index").as_str(), Span::mixed_site());
    let struct_def = quote! {pub struct #ident};
    (quote! {
        #[derive(Debug, Default)]
        #struct_def {
            #(#index_rows),*
        }
    }, ident)
}

pub fn gen_impl_def(columns: Columns, name: &Ident, row_type_name: &Ident) -> TokenStream {
    let index_rows = columns.indexes
        .into_iter()
        .map(|i| {
            let index_field_name = get_index_field_name(&i);
            quote! {
                self.#index_field_name.insert(row.#i, link);
            }
        }).collect::<Vec<_>>();

    quote! {
        impl TableIndex<#row_type_name> for #name {
            fn save_row(&self, row: #row_type_name, link: Link) {
                #(#index_rows)*
            }
        }
    }
}

pub fn get_index_field_name(ident: &Ident) -> Ident {
    Ident::new(format!("{}_index", ident).as_str(), Span::mixed_site())
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span, TokenStream};
    use quote::quote;

    use crate::worktable::gen_index_type::{gen_impl_def, gen_type_def};
    use crate::worktable::parse_columns::parse_columns;

    #[test]
    fn test_type_def() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64 index,
        }});

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens).unwrap();
        let (res, i) = gen_type_def(columns, &"Test".to_string());

        assert_eq!(i.to_string(), "TestIndex".to_string());
        assert_eq!(res.to_string(), "# [derive (Debug , Default)] pub struct TestIndex { test_index : concurrent_map :: ConcurrentMap < u64 , Link > }")
    }

    #[test]
    fn test_impl_def() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64 index,
        }});

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens).unwrap();
        let res = gen_impl_def(columns, &Ident::new("TestIndex", Span::mixed_site()), &Ident::new("TestRow", Span::mixed_site()));

        assert_eq!(res.to_string(), "impl TableIndex < TestRow > for TestIndex { fn save_row (& self , row : TestRow , link : Link) { self . test_index . insert (row . test , link) ; } ")
    }
}