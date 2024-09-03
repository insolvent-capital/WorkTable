use crate::worktable::generator::Generator;
use crate::worktable::model::PrimaryKey;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_pk_def(&mut self) -> TokenStream {
        let name = &self.name;
        let ident = Ident::new(format!("{name}PrimaryKey").as_str(), Span::mixed_site());
        let type_ = self
            .columns
            .columns_map
            .get(&self.columns.primary_key)
            .unwrap();

        let struct_def = quote! {
            pub type #ident = #type_;
        };
        self.pk = Some(PrimaryKey {
            ident,
            field: self.columns.primary_key.clone(),
            type_: type_.clone(),
        });

        struct_def
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
