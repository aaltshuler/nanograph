use arrow::datatypes::DataType;

pub type NodeId = u64;
pub type EdgeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarType {
    String,
    Bool,
    I32,
    I64,
    U32,
    U64,
    F32,
    F64,
    Date,
    DateTime,
}

impl ScalarType {
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s {
            "String" => Some(Self::String),
            "Bool" => Some(Self::Bool),
            "I32" => Some(Self::I32),
            "I64" => Some(Self::I64),
            "U32" => Some(Self::U32),
            "U64" => Some(Self::U64),
            "F32" => Some(Self::F32),
            "F64" => Some(Self::F64),
            "Date" => Some(Self::Date),
            "DateTime" => Some(Self::DateTime),
            _ => None,
        }
    }

    pub fn to_arrow(&self) -> DataType {
        match self {
            Self::String => DataType::Utf8,
            Self::Bool => DataType::Boolean,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
            Self::U32 => DataType::UInt32,
            Self::U64 => DataType::UInt64,
            Self::F32 => DataType::Float32,
            Self::F64 => DataType::Float64,
            Self::Date => DataType::Date32,
            Self::DateTime => DataType::Date64,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::I32 | Self::I64 | Self::U32 | Self::U64 | Self::F32 | Self::F64
        )
    }
}

impl std::fmt::Display for ScalarType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::String => "String",
            Self::Bool => "Bool",
            Self::I32 => "I32",
            Self::I64 => "I64",
            Self::U32 => "U32",
            Self::U64 => "U64",
            Self::F32 => "F32",
            Self::F64 => "F64",
            Self::Date => "Date",
            Self::DateTime => "DateTime",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropType {
    pub scalar: ScalarType,
    pub nullable: bool,
    pub list: bool,
    pub enum_values: Option<Vec<String>>,
}

impl PropType {
    pub fn scalar(scalar: ScalarType, nullable: bool) -> Self {
        Self {
            scalar,
            nullable,
            list: false,
            enum_values: None,
        }
    }

    pub fn list_of(scalar: ScalarType, nullable: bool) -> Self {
        Self {
            scalar,
            nullable,
            list: true,
            enum_values: None,
        }
    }

    pub fn enum_type(mut values: Vec<String>, nullable: bool) -> Self {
        values.sort();
        values.dedup();
        Self {
            scalar: ScalarType::String,
            nullable,
            list: false,
            enum_values: Some(values),
        }
    }

    pub fn is_enum(&self) -> bool {
        self.enum_values.is_some()
    }

    pub fn to_arrow(&self) -> DataType {
        let scalar_dt = self.scalar.to_arrow();
        if self.list {
            DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                "item", scalar_dt, true,
            )))
        } else {
            scalar_dt
        }
    }

    pub fn display_name(&self) -> String {
        let base = if let Some(values) = &self.enum_values {
            format!("enum({})", values.join(", "))
        } else {
            self.scalar.to_string()
        };
        let wrapped = if self.list {
            format!("[{}]", base)
        } else {
            base
        };
        if self.nullable {
            format!("{}?", wrapped)
        } else {
            wrapped
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
}
