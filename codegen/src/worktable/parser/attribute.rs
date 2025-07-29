use proc_macro2::TokenTree;
use syn::spanned::Spanned as _;

use crate::worktable::parser::Parser;

// TODO: Move this to separate attributes section because now it only parses persist.
impl Parser {
    pub fn parse_persist(&mut self) -> syn::Result<bool> {
        let Some(ident) = self.input_iter.peek().cloned() else {
            return Ok(false);
        };
        let TokenTree::Ident(ident) = ident else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        if ident.to_string().as_str() == "persist" {
            let _ = self.input_iter.next();
            self.parse_colon()?;
            let bool = self
                .input_iter
                .next()
                .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
            let res = if let TokenTree::Ident(bool) = bool {
                if bool.to_string().as_str() == "true" {
                    Ok(true)
                } else {
                    Ok(false)
                }
            } else {
                Err(syn::Error::new(bool.span(), "Expected identifier."))
            };
            self.try_parse_comma()?;

            res
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::worktable::Parser;

    #[test]
    fn test_empty() {
        let tokens = quote! {};
        let mut parser = Parser::new(tokens);
        let empty = parser.parse_persist();
        assert!(empty.is_ok());
        assert!(!empty.unwrap())
    }

    #[test]
    fn test_literal_field() {
        let tokens = quote! {"nme": TestName,};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_err());
    }

    #[test]
    fn test_persistence() {
        let tokens = quote! {persist: true,};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_ok());
        assert!(name.unwrap());
    }

    #[test]
    fn test_wrong_field() {
        let tokens = quote! {nme: TestName,};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_ok());
        assert!(!name.unwrap());
    }

    #[test]
    fn test_no_comma() {
        let tokens = quote! {name: TestName};
        let mut parser = Parser::new(tokens);
        let name = parser.parse_persist();
        assert!(name.is_ok());
        assert!(!name.unwrap());
    }
}
