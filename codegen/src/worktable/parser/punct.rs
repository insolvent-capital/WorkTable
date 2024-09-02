use syn::spanned::Spanned;
use proc_macro2::{TokenStream, TokenTree};

use crate::worktable::parser::Parser;

impl Parser {
    /// Parses ':' from [`TokenStream`].
    pub fn parse_colon(&mut self) -> syn::Result<()> {
        let iter = &mut self.input_iter;

        let colon = iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
        if let TokenTree::Punct(colon) = colon {
            if colon.as_char() != ':' {
                return Err(syn::Error::new(
                    colon.span(),
                    format!("Expected `:` found: `{}`", colon.as_char()),
                ));
            }

            Ok(())
        } else {
            Err(syn::Error::new(colon.span(), "Expected `:`."))
        }
    }

    /// Parses ',' from [`TokenStream`].
    pub fn parse_comma(&mut self) -> syn::Result<()> {
        let iter = &mut self.input_iter;

        let tt = iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), "Expected token."))?;
        comma(&tt)
    }

    /// Tries to parse ',' from [`TokenStream`] without calling `next` on wrong token.
    pub fn try_parse_comma(&mut self) -> syn::Result<()> {
        let iter = &mut self.input_iter;

        if let Some(colon) = iter.peek() {
            if let Ok(_) = comma(colon) {
                iter.next();
            }
        }

        Ok(())
    }
}

fn comma(tt: &TokenTree) -> syn::Result<()> {
    if let TokenTree::Punct(colon) = tt {
        if colon.as_char() != ',' {
            return Err(syn::Error::new(
                colon.span(),
                format!("Expected `,` found: `{}`", colon.as_char()),
            ));
        }

        Ok(())
    } else {
        Err(syn::Error::new(tt.span(), "Expected `,`."))
    }
}
