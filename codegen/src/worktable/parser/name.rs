use proc_macro2::Ident;
use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::worktable::parser::Parser;

impl Parser {
    pub fn parse_name(&mut self) -> syn::Result<Ident> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `name` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "name" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `name` field. `WorkTable` name must be specified",
                ));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        let name = self
            .input_iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
        let name = if let TokenTree::Ident(name) = name {
            name
        } else {
            return Err(syn::Error::new(name.span(), "Expected identifier."));
        };

        self.try_parse_comma()?;

        Ok(name)
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use crate::worktable::Parser;

    #[test]
    fn test_name_parse() {
        let tokens = TokenStream::from(quote! {name: TestName,});

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_ok());
        let name = name.unwrap();

        assert_eq!(name, "TestName");
    }

    #[test]
    fn test_empty() {
        let tokens = TokenStream::from(quote! {});

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }

    #[test]
    fn test_literal_field() {
        let tokens = TokenStream::from(quote! {"nme": TestName,});

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }

    #[test]
    fn test_wrong_field() {
        let tokens = TokenStream::from(quote! {nme: TestName,});

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }

    #[test]
    fn test_no_comma() {
        let tokens = TokenStream::from(quote! {name: TestName});

        let mut parser = Parser::new(tokens);
        let name = parser.parse_name();

        assert!(name.is_err());
    }
}
