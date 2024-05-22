use crate::column::IntoColumn;
use crate::Value;

pub trait WorkTableField {
    #[allow(private_bounds)]
    type Type: IntoColumn + Into<Value>;
    const INDEX: usize;
    const NAME: &'static str;
    const PRIMARY: bool = false;
}
#[macro_export]
macro_rules! field {
    (
        $index: expr, $f: ident: $ty: ty, $name: expr $(, primary = $indexed: expr)?
    ) => {
        pub struct $f;
        impl $crate::WorkTableField for $f {
            type Type = $ty;
            const INDEX: usize = $index;
            const NAME: &'static str = $name;
            $(const PRIMARY: bool = $indexed;)? // optional
        }
    };
}
