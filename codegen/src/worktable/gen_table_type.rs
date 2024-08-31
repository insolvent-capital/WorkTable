use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use crate::worktable::gen_index_type::get_index_field_name;
use crate::worktable::parse_columns::Columns;

/// Generates type alias for new [`WorkTable`].
///
/// [`WorkTable`]: worktable::WorkTable
pub fn gen_table_def(name: &String, pk_type: &Ident, row_type: &Ident, index_type: &Ident) -> (TokenStream, Ident) {
    let ident = get_table_name(&name);
    (quote! {
        #[derive(Debug, Default, Clone)]
        pub struct #ident(WorkTable<#row_type, #pk_type, #index_type>);

        impl #ident {
            pub fn select(&self, pk: #pk_type) -> Option<#row_type> {
                self.0.select(pk)
            }

            pub fn insert<const ROW_SIZE_HINT: usize>(&self, row: #row_type) -> Result<#pk_type, WorkTableError> {
                self.0.insert::<ROW_SIZE_HINT>(row)
            }

            pub fn get_next_pk(&self) -> #pk_type {
                self.0.get_next_pk()
            }
        }
    }, ident)
}

pub fn gen_table_index_impl(columns: Columns, table_ident: &Ident, row_ident: &Ident) -> TokenStream {
    let fn_defs = columns.indexes.into_iter().map(|i| {
        let type_ = columns.columns_map.get(&i).expect("exists");
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = get_index_field_name(&i);

         quote! {
             pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                 let guard = Guard::new();
                 let link = self.0.indexes.#field_ident.peek(&by, &guard)?;
                 self.0.data.select(*link).ok()
             }
         }
    }).collect::<Vec<_>>();

    quote! {
        impl #table_ident {
            #(#fn_defs)*
        }
    }
}

pub fn get_table_name(name: &String) -> Ident {
    Ident::new(format!("{}WorkTable", name).as_str(), Span::mixed_site())
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span};
    use super::gen_table_def;

    #[test]
    fn generates_name() {
        let pk_type = Ident::new("i64", Span::mixed_site());
        let row_type = Ident::new("TestRow", Span::mixed_site());
        let index_type = Ident::new("TestIndex", Span::mixed_site());

        let (tokens, name) = gen_table_def(&"Test".to_string(), &pk_type, &row_type, &index_type);
        assert_eq!(name.to_string(), "TestWorkTable".to_string());
        assert_eq!(
            tokens.to_string(),
            "type TestWorkTable = WorkTable < TestRow , i64 , TestIndex > ;"
        )
    }
}
