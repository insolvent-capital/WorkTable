use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use crate::worktable::generator::Generator;

impl Generator {
    /// Generates type alias for new [`WorkTable`].
    ///
    /// [`WorkTable`]: worktable::WorkTable
    pub fn gen_table_def(&mut self) -> TokenStream {
        let name = &self.name;
        let ident = Ident::new(format!("{}WorkTable", name).as_str(), Span::mixed_site());
        self.table_name = Some(ident.clone());

        let row_type = self.row_name.clone().unwrap();
        let pk_type = self.pk.clone().unwrap().ident;
        let index_type = self.index_name.clone().unwrap();

        quote! {
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
        }
    }

    pub fn gen_table_index_impl(&mut self) -> syn::Result<TokenStream> {
        println!("{:?}", &self.columns.indexes);
        println!("{:?}", &self.columns.columns_map);

        let fn_defs = self.columns.indexes.iter().map(|(i, idx)| {
            let type_ = self.columns.columns_map.get(&i).ok_or(syn::Error::new(
                i.span(),
                "Row not found",
            ))?;
            let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
            let field_ident = &idx.name;

            let row_ident = self.row_name.clone().unwrap();

            Ok(quote! {
                pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                    let guard = Guard::new();
                    let link = self.0.indexes.#field_ident.peek(&by, &guard)?;
                    self.0.data.select(*link).ok()
                }
            })
        }).collect::<Result<Vec<_>, syn::Error>>()?;

        let table_ident = self.table_name.clone().unwrap();
        Ok(quote! {
            impl #table_ident {
                #(#fn_defs)*
            }
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use proc_macro2::{Ident, Span, TokenStream};
//     use quote::quote;
//     use crate::worktable::generator::Generator;
//     use crate::worktable::Parser;
//
//     #[test]
//     fn generates_name() {
//         let tokens = TokenStream::from(quote! {columns: {
//             id: i64 primary_key,
//             test: u64,
//         }});
//         let mut parser = Parser::new(tokens);
//         let columns = parser.parse_columns().unwrap();
//
//         let ident = Ident::new("Test", Span::call_site());
//         let mut generator = Generator::new(ident, columns);
//         generator.gen_pk_def();
//         generator.gen_row_def();
//         generator.gen_index_def();
//
//         let tokens = generator.gen_table_def();
//         assert_eq!(generator.table_name.unwrap().to_string(), "TestWorkTable".to_string());
//         assert_eq!(
//             tokens.to_string(),
//             "type TestWorkTable = WorkTable < TestRow , i64 , TestIndex > ;"
//         )
//     }
// }
