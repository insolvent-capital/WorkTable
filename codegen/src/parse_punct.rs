use proc_macro2::token_stream;
use proc_macro2::{TokenStream, TokenTree};
use std::iter::Peekable;
use syn::spanned::Spanned;

/// Parses '{' from [`TokenStream`].
pub fn parse_left_curly_brace(
    iter: &mut impl Iterator<Item = TokenTree>,
    input: &TokenStream,
) -> syn::Result<()> {
    let curly_brace = iter.next().ok_or(syn::Error::new(
        input.span(),
        "Expected `{`. Declaration starts from curly brace",
    ))?;

    if let TokenTree::Punct(brace) = curly_brace {
        if brace.as_char() != '{' {
            return Err(syn::Error::new(
                input.span(),
                format!("Expected `{{` found: `{}`", brace.as_char()),
            ));
        }

        Ok(())
    } else {
        return Err(syn::Error::new(input.span(), "Expected `{`."));
    }
}

fn right_curly_brace(tt: &TokenTree, input: &TokenStream) -> syn::Result<()> {
    if let TokenTree::Punct(brace) = tt {
        if brace.as_char() != '}' {
            return Err(syn::Error::new(
                input.span(),
                format!("Expected `}}` found: `{}`", brace.as_char()),
            ));
        }

        Ok(())
    } else {
        return Err(syn::Error::new(input.span(), "Expected `}`."));
    }
}

/// Parses '}' from [`TokenStream`].
pub fn parse_right_curly_brace(
    iter: &mut impl Iterator<Item = TokenTree>,
    input: &TokenStream,
) -> syn::Result<()> {
    let curly_brace = iter.next().ok_or(syn::Error::new(
        input.span(),
        "Expected `}`. Declaration starts from curly brace",
    ))?;
    right_curly_brace(&curly_brace, input)
}

/// Tries to parse '}' from [`TokenStream`] without calling `next` on wrong token.
pub fn try_parse_right_curly_brace(
    iter: &mut Peekable<&mut impl Iterator<Item = TokenTree>>,
    input: &TokenStream,
) -> syn::Result<()> {
    if let Some(curly_brace) = iter.peek() {
        if let Ok(_) = right_curly_brace(curly_brace, input) {
            iter.next();
        }
    }

    Ok(())
}

/// Parses ':' from [`TokenStream`].
pub fn parse_colon(
    iter: &mut impl Iterator<Item = TokenTree>,
    input: &TokenStream,
) -> syn::Result<()> {
    let colon = iter
        .next()
        .ok_or(syn::Error::new(input.span(), "Expected token."))?;
    if let TokenTree::Punct(colon) = colon {
        if colon.as_char() != ':' {
            return Err(syn::Error::new(
                input.span(),
                format!("Expected `:` found: `{}`", colon.as_char()),
            ));
        }

        Ok(())
    } else {
        return Err(syn::Error::new(input.span(), "Expected `:`."));
    }
}

fn comma(tt: &TokenTree, input: &TokenStream) -> syn::Result<()> {
    if let TokenTree::Punct(colon) = tt {
        if colon.as_char() != ',' {
            return Err(syn::Error::new(
                input.span(),
                format!("Expected `,` found: `{}`", colon.as_char()),
            ));
        }

        Ok(())
    } else {
        return Err(syn::Error::new(input.span(), "Expected `,`."));
    }
}

/// Parses ',' from [`TokenStream`].
pub fn parse_comma(
    iter: &mut impl Iterator<Item = TokenTree>,
    input: &TokenStream,
) -> syn::Result<()> {
    let tt = iter
        .next()
        .ok_or(syn::Error::new(input.span(), "Expected token."))?;
    comma(&tt, input)
}

/// Tries to parse ',' from [`TokenStream`] without calling `next` on wrong token.
pub fn try_parse_comma(
    iter: &mut Peekable<&mut impl Iterator<Item = TokenTree>>,
    input: &TokenStream,
) -> syn::Result<()> {
    if let Some(colon) = iter.peek() {
        if let Ok(_) = comma(colon, input) {
            iter.next();
        }
    }

    Ok(())
}
