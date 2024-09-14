use proc_macro2::TokenStream;
use quote::quote;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_query_delete_impl(&mut self) -> syn::Result<TokenStream> {
        let full_row_delete = self.gen_full_row_delete();

        let table_ident = self.table_name.as_ref().unwrap();
        Ok(quote! {
            impl #table_ident {
                #full_row_delete
            }
        })
    }

    fn gen_full_row_delete(&mut self) -> TokenStream {
        let pk_ident = &self.pk.as_ref().unwrap().ident;

        quote! {
            pub async fn delete(&self, pk: #pk_ident) -> core::result::Result<(), WorkTableError> {
                let link = {
                    let guard = Guard::new();
                    *self.0.pk_map.peek(&pk, &guard).ok_or(WorkTableError::NotFound)?
                };
                let id = self.0.data.with_ref(link, |archived| {
                    archived.is_locked()
                }).map_err(WorkTableError::PagesError)?;
                if let Some(id) = id {
                    if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                        lock.as_ref().await
                    }
                }
                self.0.pk_map.remove(&pk);
                self.0.data.delete(link);

                core::result::Result::Ok(())
            }
        }
    }
}