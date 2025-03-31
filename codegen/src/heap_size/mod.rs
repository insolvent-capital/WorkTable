use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Result, Type};

pub fn expand(input: proc_macro2::TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;

    let body = match &input.data {
        Data::Struct(data_struct) => {
            let fields = match &data_struct.fields {
                Fields::Named(fields_named) => fields_named.named.iter().collect::<Vec<_>>(),
                Fields::Unnamed(fields_unnamed) => {
                    fields_unnamed.unnamed.iter().collect::<Vec<_>>()
                }
                Fields::Unit => vec![],
            };

            if fields.is_empty() {
                quote! { 0 }
            } else if fields.iter().all(|f| is_copy_primitive(&f.ty)) {
                quote! { std::mem::size_of::<Self>() }
            } else {
                let field_sizes = fields.iter().enumerate().map(|(i, f)| {
                    let accessor = match &f.ident {
                        Some(ident) => quote! { self.#ident },
                        None => {
                            let index = syn::Index::from(i);
                            quote! { self.#index }
                        }
                    };
                    quote! { size += #accessor.heap_size(); }
                });

                quote! {
                    let mut size = 0;
                    #(#field_sizes)*
                    size
                }
            }
        }
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "#[derive(HeapSize)] only supports structs",
            ));
        }
    };

    let t = quote! {
        impl HeapSize for #name {
            fn heap_size(&self) -> usize {
                #body
            }
        }
    };
    println!("{}", t);
    Ok(t)
}

fn is_copy_primitive(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Path(type_path)
            if type_path.qself.is_none() &&
               type_path.path.segments.len() == 1 &&
               matches!(
                   type_path.path.segments[0].ident.to_string().as_str(),
                   "u8" | "u16" | "u32" | "u64" | "usize" |
                   "i8" | "i16" | "i32" | "i64" | "isize" |
                   "bool" | "char" | "f64" | "f32"
               )
    )
}
