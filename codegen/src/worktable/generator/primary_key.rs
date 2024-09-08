use std::collections::HashMap;
use crate::worktable::generator::Generator;
use crate::worktable::model::PrimaryKey;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_pk_def(&mut self) -> TokenStream {
        let name = &self.name;
        let ident = Ident::new(format!("{name}PrimaryKey").as_str(), Span::mixed_site());
        let vals = self.columns.primary_keys.iter().map(|i| {
            (i.clone(), self.columns.columns_map.get(i).unwrap().clone())
        }).collect::<HashMap<_, _>>();

        let def = if vals.len() == 1 {
            let type_ = vals.values().next().unwrap();
            quote! {
                pub type #ident = #type_;
            }
        } else {
            let types = vals.values();
            quote! {
                pub type #ident = (#(#types),*);
            }
        };

        self.pk = Some(PrimaryKey {
            ident,
            vals
        });

        def
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

        let pk_def = generator.gen_pk_def();

        assert_eq!(generator.pk.unwrap().ident.to_string(), "TestPrimaryKey");
        assert_eq!(pk_def.to_string(), "pub type TestPrimaryKey = i64 ;");
    }

    #[test]
    fn test_row_generation_multiple() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64 primary_key,
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns().unwrap();

        let ident = Ident::new("Test", Span::call_site());
        let mut generator = Generator::new(ident, columns);

        let pk_def = generator.gen_pk_def();

        assert_eq!(generator.pk.unwrap().ident.to_string(), "TestPrimaryKey");
        assert_eq!(
            pk_def.to_string(),
            "pub type TestPrimaryKey = (i64 , u64) ;"
        );
    }
}
