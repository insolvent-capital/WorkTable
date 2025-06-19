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
        while self.peek_next().is_some() {
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

                    self.try_parse_comma()?;

                    let value = value.to_string();
                    let value = value.replace("_", "");

                    config.page_size = Some(u32::from_str(value.as_str()).unwrap())
                }
                "row_derives" => {
                    const CONFIG_VARIANTS: [&str; 2] = ["page_size", "row_derives"];

                    let mut derives = vec![];

                    while let Some(ident) = self.peek_next() {
                        if CONFIG_VARIANTS.contains(&ident.to_string().as_str()) {
                            if derives.is_empty() {
                                return Err(syn::Error::new(
                                    ident.span(),
                                    "Expected at least one derive in declaration.",
                                ));
                            }
                            break;
                        }

                        let derive = self.input_iter.next().ok_or(syn::Error::new(
                            self.input.span(),
                            "Expected at least one derive in declaration",
                        ))?;
                        let derive = if let TokenTree::Ident(derive) = derive {
                            derive
                        } else {
                            return Err(syn::Error::new(derive.span(), "Expected identifier."));
                        };

                        self.try_parse_comma()?;

                        derives.push(derive)
                    }

                    config.row_derives = derives;
                }
                _ => return Err(syn::Error::new(name.span(), "Unexpected identifier")),
            }
        }

        Ok(Some(()))
    }
}
