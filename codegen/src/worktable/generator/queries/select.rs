use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

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
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

        quote! {
            pub fn select_all(&self) -> SelectQueryBuilder<#row_ident,
                                                           impl DoubleEndedIterator<Item = #row_ident> + '_ + Sized,
                                                           #column_range_type,
                                                           #row_fields_ident>
            {
                let iter = self.0.pk_map
                    .iter()
                    .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok());

                SelectQueryBuilder::new(iter)
            }
        }
    }
}
