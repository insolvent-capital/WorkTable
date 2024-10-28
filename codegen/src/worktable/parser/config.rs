use crate::worktable::model::{Config, Index};
use crate::worktable::Parser;
use proc_macro2::{Delimiter, Ident, TokenTree};
use std::collections::HashMap;
use syn::spanned::Spanned;

const CONFIG_FIELD_NAME: &str = "config";

impl Parser {
    pub fn parse_configs(&mut self) -> syn::Result<Config> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            format!("Expected `{}` field in declaration", CONFIG_FIELD_NAME),
        ))?;

        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != CONFIG_FIELD_NAME {
                return Err(syn::Error::new(ident.span(), format!("Expected `{}` field in declaration", CONFIG_FIELD_NAME)));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        let tt = {
            let group = self.input_iter.next().ok_or(syn::Error::new(
                self.input.span(),
                format!("Expected `{}` declarations", CONFIG_FIELD_NAME),
            ))?;
            if let TokenTree::Group(group) = group {
                if group.delimiter() != Delimiter::Brace {
                    return Err(syn::Error::new(group.span(), "Expected brace"));
                }
                group.stream()
            } else {
                return Err(syn::Error::new(
                    group.span(),
                    format!("Expected `{}` declarations", CONFIG_FIELD_NAME),
                ));
            }
        };

        let mut parser = Parser::new(tt);
        parser.parse_config()
    }

    pub fn parse_config(&mut self) -> syn::Result<Config> {
        Ok(Config {})
    }
}

#[cfg(test)]
mod tests {
    use crate::worktable::Parser;

    use proc_macro2::TokenStream;
    use quote::quote;

    #[test]
    fn test_indexes_parse() {
        let tokens = TokenStream::from(quote! {config: {}});
        let mut parser = Parser::new(tokens);
        let configs = parser.parse_configs();

        assert!(configs.is_ok());
        let columns = configs.unwrap();
    }
}
