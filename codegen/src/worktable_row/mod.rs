
use proc_macro2::{Literal, TokenStream};
use quote::quote;
use syn::spanned::Spanned;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_struct = match syn::parse2::<syn::ItemStruct>(input.clone()) {
        Ok(data) => data,
        Err(err) => {
            return Err(syn::Error::new(input.span(), err.to_string()));
        }
    };


    Ok(quote! {}.into())
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use crate::worktable_row::expand;

    #[test]
    fn test() {
        let typ = quote! {struct Test;};

        let res = expand(typ).unwrap();
    }
}