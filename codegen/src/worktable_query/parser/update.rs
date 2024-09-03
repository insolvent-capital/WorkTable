use proc_macro2::TokenTree;
use syn::spanned::Spanned;
use crate::worktable_query::model::Operation;
use crate::worktable_query::Parser;

impl Parser {
    pub fn parse_updates(&mut self) -> syn::Result<Vec<Operation>> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `update` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "update" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `update` field. `WorkTable` name must be specified",
                ));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        let ops = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation declarations",
        ))?;
        if let TokenTree::Group(ops) = ops {
            let mut parser = Parser::new(ops.stream());
            let mut ops = Vec::new();
            while parser.has_next() {
                let row = parser.parse_operation()?;
                ops.push(row);
                parser.try_parse_comma()?
            }
            Ok(ops)
        } else {
            Err(syn::Error::new(
                ops.span(),
                "Expected operation declarations",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use crate::worktable_query::Parser;

    #[test]
    fn test_update() {
        let tokens = quote! {
            update: {
                TestQuery(id, test) by name,
                Test1Query(id, name) by test,
            }
        };
        let mut parser = Parser::new(tokens);
        let ops = parser.parse_updates().unwrap();

        assert_eq!(ops.len(), 2);
        let op = &ops[0];

        assert_eq!(op.name, "TestQuery");
        assert_eq!(op.columns.len(), 2);
        assert_eq!(op.columns[0], "id");
        assert_eq!(op.columns[1], "test");
        assert_eq!(op.by, "name");
    }
}