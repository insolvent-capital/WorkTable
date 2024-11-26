use std::str::FromStr;

use proc_macro2::{Delimiter, TokenTree};
use syn::spanned::Spanned;

use crate::worktable::model::Config;
use crate::worktable::Parser;

const CONFIG_FIELD_NAME: &str = "config";

impl Parser {
    pub fn parse_configs(&mut self) -> syn::Result<Config> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            format!("Expected `{}` field in declaration", CONFIG_FIELD_NAME),
        ))?;

        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != CONFIG_FIELD_NAME {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("Expected `{}` field in declaration", CONFIG_FIELD_NAME),
                ));
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
        let mut config = Config::default();
        parser.parse_config(&mut config)?;

        Ok(config)
    }

    pub fn parse_config(&mut self, config: &mut Config) -> syn::Result<Option<()>> {
        let Some(_) = self.input_iter.peek() else {
            return Ok(None);
        };
        let ident = self.input_iter.next().unwrap();
        let name = if let TokenTree::Ident(ident) = ident {
            ident
        } else {
            return Err(syn::Error::new(ident.span(), "Expected identifier."));
        };

        self.parse_colon()?;

        match name.to_string().as_str() {
            "page_size" => {
                let value = self.input_iter.next().ok_or(syn::Error::new(
                    self.input.span(),
                    "Expected page size value in declaration",
                ))?;
                let value = if let TokenTree::Literal(value) = value {
                    value
                } else {
                    return Err(syn::Error::new(value.span(), "Expected identifier."));
                };
                let value = value.to_string();
                let value = value.replace("_", "");

                config.page_size = Some(u32::from_str(value.as_str()).unwrap())
            }
            _ => return Err(syn::Error::new(name.span(), "Unexpected identifier")),
        }

        Ok(Some(()))
    }
}

#[cfg(test)]
mod tests {
    use crate::worktable::Parser;

    use proc_macro2::TokenStream;
    use quote::quote;

    #[test]
    fn test_indexes_parse() {
        let tokens = TokenStream::from(quote! {config: {
            page_size: 16_000
        }});
        let mut parser = Parser::new(tokens);
        let configs = parser.parse_configs();

        assert!(configs.is_ok());
        let columns = configs.unwrap();
    }
}
