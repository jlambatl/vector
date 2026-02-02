use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder;
use vector_config::configurable_component;
use vector_core::{config::DataType, event::Event, schema};

use crate::avro::AvroEncoding;
use crate::encoding::BuildError;

/// OCF (Object Container File) magic bytes: 'O', 'b', 'j', 1
const OCF_MAGIC_BYTES: &[u8] = b"Obj\x01";
const OCF_SYNC_MARKER: [u8; 16] = [
    0xc3, 0x01, 0x95, 0x6a, 0x7c, 0x8e, 0x4d, 0xb2, 0xa1, 0x3f, 0x5c, 0x72, 0x0e, 0x9d, 0x4b, 0x8f,
];

/// Config used to build a `AvroSerializer`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AvroSerializerConfig {
    /// Options for the Avro serializer.
    pub avro: AvroSerializerOptions,
}

impl AvroSerializerConfig {
    /// Creates a new `AvroSerializerConfig`.
    pub const fn new(schema: String) -> Self {
        Self::new_with_encoding(schema, None)
    }

    /// Creates a new `AvroSerializerConfig` with a specific encoding format.
    pub const fn new_with_encoding(schema: String, encoding: Option<AvroEncoding>) -> Self {
        Self {
            avro: AvroSerializerOptions {
                schema,
                encoding: match encoding {
                    Some(enc) => enc,
                    None => AvroEncoding::Datum,
                },
            },
        }
    }

    /// Build the `AvroSerializer` from this configuration.
    pub fn build(&self) -> Result<AvroSerializer, BuildError> {
        let schema = apache_avro::Schema::parse_str(&self.avro.schema)
            .map_err(|error| format!("Failed building Avro serializer: {error}"))?;
        Ok(AvroSerializer::new(schema, self.avro.encoding))
    }

    /// The data type of events that are accepted by `AvroSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // TODO: Convert the Avro schema to a vector schema requirement.
        schema::Requirement::empty()
    }
}

/// Apache Avro serializer options.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct AvroSerializerOptions {
    /// The Avro schema definition in JSON format.
    #[configurable(metadata(
        docs::examples = r#"{ "type": "record", "name": "log", "fields": [{ "name": "message", "type": "string" }] }"#
    ))]
    #[configurable(metadata(docs::human_name = "Schema JSON"))]
    pub schema: String,

    /// The encoding format to use.
    ///
    /// Defaults to `datum` for backward compatibility.
    #[serde(default)]
    #[configurable(metadata(docs::examples = "datum", docs::examples = "object_container_file"))]
    pub encoding: AvroEncoding,
}

/// Serializer that converts an `Event` to bytes using the Apache Avro format.
#[derive(Debug)]
pub struct AvroSerializer {
    schema: apache_avro::Schema,
    encoding: AvroEncoding,
    /// Tracks whether the OCF header has been written (only used for ObjectContainerFile encoding).
    ocf_header_written: bool,
    /// The sync marker used for OCF format (16 bytes).
    sync_marker: [u8; 16],
}

impl Clone for AvroSerializer {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            encoding: self.encoding,
            ocf_header_written: false, // Each clone starts with header not written
            sync_marker: self.sync_marker,
        }
    }
}

impl AvroSerializer {
    /// Creates a new `AvroSerializer`.
    pub fn new(schema: apache_avro::Schema, encoding: AvroEncoding) -> Self {
        Self {
            schema,
            encoding,
            ocf_header_written: false,
            sync_marker: OCF_SYNC_MARKER,
        }
    }

    /// Writes the OCF file header to the buffer.
    fn write_ocf_header(&mut self, buffer: &mut BytesMut) -> Result<(), vector_common::Error> {
        // Write header only once per encoder instance
        if !self.ocf_header_written {
            self.ocf_header_written = true;

            // OCF magic bytes: 'O', 'b', 'j', 1
            buffer.put_slice(OCF_MAGIC_BYTES);

            // Build metadata map with schema and codec
            let mut metadata: std::collections::HashMap<String, apache_avro::types::Value> =
                std::collections::HashMap::new();
            metadata.insert(
                "avro.schema".to_string(),
                apache_avro::types::Value::Bytes(self.schema.canonical_form().into_bytes()),
            );
            // avro.codec is required by the Avro OCF specification:
            // https://avro.apache.org/docs/1.11.1/specification/#object-container-files
            // "null" indicates uncompressed data blocks
            metadata.insert(
                "avro.codec".to_string(),
                apache_avro::types::Value::Bytes(b"null".to_vec()),
            );

            // Encode metadata as Avro map
            let metadata_value = apache_avro::types::Value::Map(metadata);
            let metadata_schema = apache_avro::Schema::Map(apache_avro::schema::MapSchema {
                types: Box::new(apache_avro::Schema::Bytes),
                attributes: Default::default(),
            });
            let metadata_bytes = apache_avro::to_avro_datum(&metadata_schema, metadata_value)?;
            buffer.put_slice(&metadata_bytes);

            // Write 16-byte sync marker
            buffer.put_slice(&self.sync_marker);
        }

        Ok(())
    }

    /// Writes a data block for OCF format.
    fn write_ocf_block(
        &self,
        buffer: &mut BytesMut,
        value: apache_avro::types::Value,
    ) -> Result<(), vector_common::Error> {
        // Encode the object
        let object_bytes = apache_avro::to_avro_datum(&self.schema, value)?;

        // Write block: object count (varint), block size (varint), data, sync marker
        let count: i64 = 1;
        let size: i64 = object_bytes.len() as i64;

        // Encode count as varint
        let count_bytes = apache_avro::to_avro_datum(
            &apache_avro::Schema::Long,
            apache_avro::types::Value::Long(count),
        )?;
        buffer.put_slice(&count_bytes);

        // Encode size as varint
        let size_bytes = apache_avro::to_avro_datum(
            &apache_avro::Schema::Long,
            apache_avro::types::Value::Long(size),
        )?;
        buffer.put_slice(&size_bytes);

        // Write serialized object data
        buffer.put_slice(&object_bytes);

        // Write sync marker
        buffer.put_slice(&self.sync_marker);

        Ok(())
    }
}

impl Encoder<Event> for AvroSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let log = event.into_log();
        let value = apache_avro::to_value(log)?;
        let value = value.resolve(&self.schema)?;

        match self.encoding {
            AvroEncoding::Datum => {
                // Original single-object datum encoding
                let bytes = apache_avro::to_avro_datum(&self.schema, value)?;
                buffer.put_slice(&bytes);
            }
            AvroEncoding::ObjectContainerFile => {
                // Object Container File format
                self.write_ocf_header(buffer)?;
                self.write_ocf_block(buffer, value)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use indoc::indoc;
    use vector_core::event::{LogEvent, Value};
    use vrl::btreemap;

    use super::*;

    #[test]
    fn serialize_avro() {
        let event = Event::Log(LogEvent::from(btreemap! {
            "foo" => Value::from("bar")
        }));
        let schema = indoc! {r#"
            {
                "type": "record",
                "name": "Log",
                "fields": [
                    {
                        "name": "foo",
                        "type": ["string"]
                    }
                ]
            }
        "#}
        .to_owned();
        let config = AvroSerializerConfig::new(schema);
        let mut serializer = config.build().unwrap();
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        assert_eq!(bytes.freeze(), b"\0\x06bar".as_slice());
    }

    #[test]
    fn serialize_avro_ocf() {
        let event = Event::Log(LogEvent::from(btreemap! {
            "foo" => Value::from("bar")
        }));
        let schema = indoc! {r#"
            {
                "type": "record",
                "name": "Log",
                "fields": [
                    {
                        "name": "foo",
                        "type": ["string"]
                    }
                ]
            }
        "#}
        .to_owned();
        let config = AvroSerializerConfig::new_with_encoding(
            schema,
            Some(AvroEncoding::ObjectContainerFile),
        );
        let mut serializer = config.build().unwrap();
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        let result = bytes.freeze();
        // Verify OCF magic bytes
        assert_eq!(&result[0..4], OCF_MAGIC_BYTES);
        // OCF format includes header, so output should be larger than datum format
        assert!(
            result.len() > 50,
            "OCF format should include header metadata"
        );
    }

    #[test]
    fn serialize_avro_ocf_multiple_events() {
        let schema = indoc! {r#"
            {
                "type": "record",
                "name": "Log",
                "fields": [
                    {
                        "name": "message",
                        "type": ["string"]
                    }
                ]
            }
        "#}
        .to_owned();
        let config = AvroSerializerConfig::new_with_encoding(
            schema,
            Some(AvroEncoding::ObjectContainerFile),
        );
        let mut serializer = config.build().unwrap();
        let mut bytes = BytesMut::new();

        let event1 = Event::Log(LogEvent::from(btreemap! {
            "message" => Value::from("first")
        }));
        let event2 = Event::Log(LogEvent::from(btreemap! {
            "message" => Value::from("second")
        }));

        serializer.encode(event1, &mut bytes).unwrap();
        let size_after_first = bytes.len();

        serializer.encode(event2, &mut bytes).unwrap();
        let size_after_second = bytes.len();

        // Verify header is written only once (second event adds less data)
        let first_event_size = size_after_first;
        let second_event_added = size_after_second - size_after_first;
        assert!(
            second_event_added < first_event_size,
            "Second event should not include header (added {} bytes vs first event {} bytes)",
            second_event_added,
            first_event_size
        );

        // Verify OCF magic bytes at start
        assert_eq!(&bytes[0..4], OCF_MAGIC_BYTES);
    }

    #[test]
    fn encoding_default_is_datum() {
        let schema = r#"{"type": "record", "name": "Log", "fields": []}"#.to_string();
        let config = AvroSerializerConfig::new(schema);
        assert_eq!(config.avro.encoding, AvroEncoding::Datum);
    }
}
