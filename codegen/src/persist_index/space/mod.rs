mod events;
mod index;

use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_index::generator::Generator;

impl Generator {
    pub fn gen_space_index(&self) -> TokenStream {
        let secondary_index = self.gen_space_secondary_index_type();
        let secondary_impl = self.gen_space_secondary_index_impl_space_index();
        let secondary_index_events = self.gen_space_secondary_index_events_type();
        let secondary_index_events_impl = self.gen_space_secondary_index_events_impl();

        quote! {
            #secondary_index_events
            #secondary_index_events_impl
            #secondary_index
            #secondary_impl
        }
    }
}
