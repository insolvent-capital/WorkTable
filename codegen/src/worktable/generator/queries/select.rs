use crate::worktable::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;
use crate::name_generator::WorktableNameGenerator;

impl Generator {
    pub fn gen_query_select_impl(&mut self) -> syn::Result<TokenStream> {
        let select_all = self.gen_select_all();

        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();

        Ok(quote! {
            impl #table_ident {
                #select_all
            }
        })
    }

    fn gen_select_all(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();

        quote! {
            pub fn select_all<'a>(&'a self) -> SelectQueryBuilder<'a, #row_ident, Self> {
                SelectQueryBuilder::new(&self)
            }
        }
    }
}
