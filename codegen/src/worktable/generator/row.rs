use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_row_def(&mut self) -> TokenStream {
        let name = &self.name;

        let ident = Ident::new(format!("{name}Row").as_str(), Span::mixed_site());
        let struct_def = quote! {pub struct #ident};

        let pk = self.pk.clone().unwrap();
        let pk_ident = &pk.ident;
        let pk_field = &pk.field;

        let row_impl = quote! {
            impl TableRow<#pk_ident> for #ident {
                const ROW_SIZE: usize = ::core::mem::size_of::<#ident>();

                fn get_primary_key(&self) -> &#pk_ident {
                    &self.#pk_field
                }
            }
        };

        let rows: Vec<_> = self
            .columns
            .columns_map
            .iter()
            .map(|(name, type_)| {
                quote! {pub #name: #type_,}
            })
            .collect();

        self.row_name = Some(ident);
        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq, Eq, Hash)]
            #[archive(compare(PartialEq))]
            #[archive_attr(derive(Debug))]
            #[repr(C)]
            #struct_def {
                #(#rows)*
            }

            #row_impl
        }
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span, TokenStream};
    use quote::quote;

    use crate::worktable::generator::Generator;
    use crate::worktable::Parser;

    #[test]
    fn test_row_generation() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64,
        }});
        let mut parser = Parser::new(tokens);

        let columns = parser.parse_columns().unwrap();

        let ident = Ident::new("Test", Span::call_site());
        let mut generator = Generator::new(ident, columns);

        let pk = generator.gen_pk_def();
        let row_def = generator.gen_row_def();

        assert_eq!(generator.row_name.unwrap().to_string(), "TestRow");
        assert_eq!(row_def.to_string(), "TestRow");
    }
}
