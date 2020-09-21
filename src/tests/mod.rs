use crate::messages;
use crate::schemata::Column;
use crate::ORCFile;
mod codecs;

#[test]
fn test_read_postscript() {
    let orc_bytes = include_bytes!("sample.orc");
    let ps = crate::toc::read_postscript(orc_bytes).unwrap();
    assert_eq!(ps.magic(), "ORC");
    assert_eq!(ps.footer_length(), 584);
    assert_eq!(ps.compression(), messages::CompressionKind::Zlib);
    assert_eq!(ps.compression_block_size(), 131072);
    assert_eq!(ps.writer_version(), 4);
    assert_eq!(ps.metadata_length(), 331);
    assert_eq!(ps.stripe_statistics_length(), 0);
}

#[test]
fn test_read_footer() {
    use crate::schemata::Schema as Sch;
    let orc_bytes = include_bytes!("sample.orc");
    let orc_toc = crate::toc::ORCFile::from_slice(&orc_bytes[..]).unwrap();
    // The metadata is empty
    assert_eq!(orc_toc.user_metadata(), hashmap! {});
    // This is a bit verbose but the point is to stress test the type system
    assert_eq!(
        orc_toc.schema(),
        &[
            ("_col0".into(), Sch::Boolean { id: 1 }),
            ("_col1".into(), Sch::Byte { id: 2 }),
            ("_col2".into(), Sch::Short { id: 3 }),
            ("_col3".into(), Sch::Int { id: 4 }),
            ("_col4".into(), Sch::Long { id: 5 }),
            ("_col5".into(), Sch::Float { id: 6 }),
            ("_col6".into(), Sch::Double { id: 7 }),
            (
                "_col7".into(),
                Sch::Decimal {
                    id: 8,
                    precision: 10,
                    scale: 0
                }
            ),
            ("_col8".into(), Sch::Char { id: 9, length: 1 }),
            ("_col9".into(), Sch::Char { id: 10, length: 3 }),
            ("_col10".into(), Sch::String { id: 11 }),
            ("_col11".into(), Sch::Varchar { id: 12, length: 10 }),
            ("_col12".into(), Sch::Binary { id: 13 }),
            ("_col13".into(), Sch::Binary { id: 14 }),
            ("_col14".into(), Sch::Date { id: 15 }),
            ("_col15".into(), Sch::Timestamp { id: 16 }),
            (
                "_col16".into(),
                Sch::List {
                    id: 17,
                    inner: Box::new(Sch::Int { id: 18 })
                }
            ),
            (
                "_col17".into(),
                Sch::List {
                    id: 19,
                    inner: Box::new(Sch::List {
                        id: 20,
                        inner: Box::new(Sch::Int { id: 21 })
                    })
                }
            ),
            (
                "_col18".into(),
                Sch::Struct {
                    id: 22,
                    fields: vec![
                        ("city".into(), Sch::String { id: 23 }),
                        ("population".into(), Sch::Int { id: 24 })
                    ]
                }
            ),
            (
                "_col19".into(),
                Sch::Struct {
                    id: 25,
                    fields: vec![
                        (
                            "city".into(),
                            Sch::Struct {
                                id: 26,
                                fields: vec![
                                    ("name".into(), Sch::String { id: 27 }),
                                    ("population".into(), Sch::Int { id: 28 })
                                ]
                            }
                        ),
                        ("state".into(), Sch::String { id: 29 })
                    ]
                }
            ),
            (
                "_col20".into(),
                Sch::List {
                    id: 30,
                    inner: Box::new(Sch::Struct {
                        id: 31,
                        fields: vec![
                            ("city".into(), Sch::String { id: 32 }),
                            ("population".into(), Sch::Int { id: 33 })
                        ]
                    })
                }
            ),
            (
                "_col21".into(),
                Sch::Struct {
                    id: 34,
                    fields: vec![
                        (
                            "city".into(),
                            Sch::List {
                                id: 35,
                                inner: Box::new(Sch::String { id: 36 })
                            }
                        ),
                        (
                            "population".into(),
                            Sch::List {
                                id: 37,
                                inner: Box::new(Sch::Int { id: 38 })
                            }
                        )
                    ]
                }
            ),
            (
                "_col22".into(),
                Sch::Map {
                    id: 39,
                    key: Box::new(Sch::Int { id: 41 }),
                    value: Box::new(Sch::String { id: 40 })
                }
            ),
            (
                "_col23".into(),
                Sch::Map {
                    id: 42,
                    key: Box::new(Sch::Map {
                        id: 44,
                        key: Box::new(Sch::Int { id: 46 }),
                        value: Box::new(Sch::String { id: 45 })
                    }),
                    value: Box::new(Sch::String { id: 43 })
                }
            )
        ]
    );
}

// #[test]
// fn test_stripe() {
//     use crate::codecs::Codec;
//     use crate::schemata::{ColumnRef, Encoded, ColumnSpec as TS};
//     let orc_bytes = include_bytes!("sample.orc");
//     let mut orc_toc = crate::toc::ORCFile::from_slice(&orc_bytes[..]).unwrap();
//     let schema = orc_toc.flat_schema().to_vec();
//     let ref stripe = orc_toc.stripe(0).unwrap();
//     assert_eq!(stripe.number_of_rows(), 3);
//     assert_eq!(stripe.columns().unwrap(), vec![
//         ColumnRef { stripe, schema: schema[0].clone(), present: None, content: TS::Struct() },
//         ColumnRef { stripe, schema: schema[1].clone(), present: Some(Encoded(Codec::BooleanRLE, 1149..1154)), content: TS::Boolean { data: Encoded(Codec::BooleanRLE, 1154..1159) } },
//         ColumnRef { stripe, schema: schema[2].clone(), present: Some(Encoded(Codec::BooleanRLE, 1159..1164)), content: TS::Byte { data: Encoded(Codec::ByteRLE, 1164..1170) } },
//         ColumnRef { stripe, schema: schema[3].clone(), present: Some(Encoded(Codec::BooleanRLE, 1170..1175)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1175..1182) } },
//         ColumnRef { stripe, schema: schema[4].clone(), present: Some(Encoded(Codec::BooleanRLE, 1182..1187)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1187..1194) } },
//         ColumnRef { stripe, schema: schema[5].clone(), present: Some(Encoded(Codec::BooleanRLE, 1194..1199)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1199..1206) } },
//         ColumnRef { stripe, schema: schema[6].clone(), present: Some(Encoded(Codec::BooleanRLE, 1206..1211)), content: TS::Float { data: Encoded(Codec::FloatEnc, 1211..1222) } },
//         ColumnRef { stripe, schema: schema[7].clone(), present: Some(Encoded(Codec::BooleanRLE, 1222..1227)), content: TS::Float { data: Encoded(Codec::FloatEnc, 1227..1242) } },
//         ColumnRef { stripe, schema: schema[8].clone(), present: Some(Encoded(Codec::BooleanRLE, 1242..1247)), content: TS::Decimal { data: Encoded(Codec::IntRLE2, 1247..1252), scale: Encoded(Codec::IntRLE2, 1252..1258) } },
//         ColumnRef { stripe, schema: schema[9].clone(), present: Some(Encoded(Codec::BooleanRLE, 1258..1263)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1263..1268), length: Encoded(Codec::UintRLE2, 1268..1274) } },
//         ColumnRef { stripe, schema: schema[10].clone(), present: Some(Encoded(Codec::BooleanRLE, 1274..1279)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1279..1288), length: Encoded(Codec::UintRLE2, 1288..1294) } },
//         ColumnRef { stripe, schema: schema[11].clone(), present: Some(Encoded(Codec::BooleanRLE, 1294..1299)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1299..1305), length: Encoded(Codec::UintRLE2, 1305..1311) } },
//         ColumnRef { stripe, schema: schema[12].clone(), present: Some(Encoded(Codec::BooleanRLE, 1311..1316)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1316..1322), length: Encoded(Codec::UintRLE2, 1322..1328) } },
//         ColumnRef { stripe, schema: schema[13].clone(), present: Some(Encoded(Codec::BooleanRLE, 1328..1333)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1333..1339), length: Encoded(Codec::UintRLE2, 1339..1345) } },
//         ColumnRef { stripe, schema: schema[14].clone(), present: Some(Encoded(Codec::BooleanRLE, 1345..1350)), content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1350..1358), length: Encoded(Codec::UintRLE2, 1358..1364) } },
//         ColumnRef { stripe, schema: schema[15].clone(), present: Some(Encoded(Codec::BooleanRLE, 1364..1369)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1369..1380) } },
//         ColumnRef { stripe, schema: schema[16].clone(), present: Some(Encoded(Codec::BooleanRLE, 1380..1385)), content: TS::Timestamp { seconds: Encoded(Codec::IntRLE2, 1385..1398), nanos: Encoded(Codec::NanosEnc2, 1398..1407) } },
//         ColumnRef { stripe, schema: schema[17].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1407..1412) } },
//         ColumnRef { stripe, schema: schema[18].clone(), present: Some(Encoded(Codec::BooleanRLE, 1412..1418)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1418..1428) } },
//         ColumnRef { stripe, schema: schema[19].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1428..1433) } },
//         ColumnRef { stripe, schema: schema[20].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1433..1438) } },
//         ColumnRef { stripe, schema: schema[21].clone(), present: Some(Encoded(Codec::BooleanRLE, 1438..1444)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1444..1454) } },
//         ColumnRef { stripe, schema: schema[22].clone(), present: None, content: TS::Struct() },
//         ColumnRef { stripe, schema: schema[23].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1454..1459), length: Encoded(Codec::UintRLE2, 1459..1465) } },
//         ColumnRef { stripe, schema: schema[24].clone(), present: Some(Encoded(Codec::BooleanRLE, 1479..1484)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1484..1491) } },
//         ColumnRef { stripe, schema: schema[25].clone(), present: None, content: TS::Struct() },
//         ColumnRef { stripe, schema: schema[26].clone(), present: None, content: TS::Struct() },
//         ColumnRef { stripe, schema: schema[27].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1491..1496), length: Encoded(Codec::UintRLE2, 1496..1502) } },
//         ColumnRef { stripe, schema: schema[28].clone(), present: Some(Encoded(Codec::BooleanRLE, 1516..1521)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1521..1528) } },
//         ColumnRef { stripe, schema: schema[29].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1528..1533), length: Encoded(Codec::UintRLE2, 1533..1539) } },
//         ColumnRef { stripe, schema: schema[30].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1544..1549) } },
//         ColumnRef { stripe, schema: schema[31].clone(), present: None, content: TS::Struct() },
//         ColumnRef { stripe, schema: schema[32].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1549..1555), length: Encoded(Codec::UintRLE2, 1555..1561) } },
//         ColumnRef { stripe, schema: schema[33].clone(), present: Some(Encoded(Codec::BooleanRLE, 1582..1587)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1587..1600) } },
//         ColumnRef { stripe, schema: schema[34].clone(), present: None, content: TS::Struct() },
//         ColumnRef { stripe, schema: schema[35].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1600..1605) } },
//         ColumnRef { stripe, schema: schema[36].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1605..1611), length: Encoded(Codec::UintRLE2, 1611..1617) } },
//         ColumnRef { stripe, schema: schema[37].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1638..1643) } },
//         ColumnRef { stripe, schema: schema[38].clone(), present: Some(Encoded(Codec::BooleanRLE, 1643..1648)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1648..1661) } },
//         ColumnRef { stripe, schema: schema[39].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1661..1666) } },
//         ColumnRef { stripe, schema: schema[40].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1666..1672), length: Encoded(Codec::UintRLE2, 1672..1678) } },
//         ColumnRef { stripe, schema: schema[41].clone(), present: Some(Encoded(Codec::BooleanRLE, 1690..1695)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1695..1708) } },
//         ColumnRef { stripe, schema: schema[42].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1708..1713) } },
//         ColumnRef { stripe, schema: schema[43].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1713..1718), length: Encoded(Codec::UintRLE2, 1718..1724) } },
//         ColumnRef { stripe, schema: schema[44].clone(), present: None, content: TS::Container { length: Encoded(Codec::UintRLE2, 1731..1736) } },
//         ColumnRef { stripe, schema: schema[45].clone(), present: None, content: TS::Blob { data: Encoded(Codec::BinaryEnc, 1736..1741), length: Encoded(Codec::UintRLE2, 1741..1747) } },
//         ColumnRef { stripe, schema: schema[46].clone(), present: Some(Encoded(Codec::BooleanRLE, 1755..1760)), content: TS::Int { data: Encoded(Codec::IntRLE2, 1760..1769) } }
//     ]);
// }

#[test]
fn read_column() {
    let orc_bytes = include_bytes!("sample.orc");
    let ref mut orc_toc = crate::ORCFile::from_slice(&orc_bytes[..]).unwrap();
    let stripe = orc_toc.stripe(0).unwrap();
    if let Column::Boolean { data, .. } = stripe.column(1, orc_toc).unwrap() {
        assert_eq!(data, vec![true, true, false]);
    } else {
        panic!("Why is there no boolean in this sample");
    }
}

#[test]
fn go_read_empty_file() {
    let orc_bytes = include_bytes!("examples/TestOrcFile.emptyFile.orc");
    ORCFile::from_slice(&orc_bytes[..]).expect_err("ORC should have been empty");
}

#[test]
fn go_read_metadata() {
    let orc_bytes = include_bytes!("examples/TestOrcFile.metaData.orc");
    let ref mut toc = ORCFile::from_slice(&orc_bytes[..]).unwrap();
    let column = toc.stripe(0).unwrap().dataframe(toc).unwrap();
    assert_eq!(toc.user_metadata(), hashmap! {"foo".into() => "bar".into()});
}

/// The simplest test - a tiny table. This one without using JSON.
#[test]
fn go_read_test1() {
    let orc_bytes = include_bytes!("examples/TestOrcFile.test1.orc");
    let ref mut toc = ORCFile::from_slice(&orc_bytes[..]).unwrap();
    let frame = toc.stripe(0).unwrap().dataframe(toc).unwrap();

    assert_eq!(
        frame.columns["boolean1"].to_booleans(),
        Some(vec![Some(false), Some(true)])
    );
    assert_eq!(
        frame.columns["byte1"].to_all_numbers(),
        Some(vec![1u8, 100])
    );
    assert_eq!(
        frame.columns["short1"].to_all_numbers(),
        Some(vec![1024i16, 2048])
    );
    assert_eq!(
        frame.columns["int1"].to_all_numbers(),
        Some(vec![65536i32, 65536])
    );
    assert_eq!(
        frame.columns["long1"].to_all_numbers(),
        Some(vec![9223372036854775807i64, 9223372036854775807])
    );
    assert_eq!(
        frame.columns["float1"].to_all_numbers(),
        Some(vec![1f32, 2.])
    );
    assert_eq!(
        frame.columns["double1"].to_all_numbers(),
        Some(vec![-15f64, -5.])
    );
    assert_eq!(
        frame.columns["bytes1"].as_slices().unwrap(),
        vec![Some(&[0, 1, 2, 3, 4][..]), Some(&[])]
    );
    assert_eq!(
        frame.columns["bytes1"].to_vecs().unwrap(),
        vec![Some(vec![0, 1, 2, 3, 4]), Some(vec![])]
    );
    assert_eq!(
        frame.columns["string1"].as_strs().unwrap(),
        vec![Some("hi"), Some("bye")]
    );
    assert_eq!(
        frame.columns["string1"].to_strings().unwrap(),
        vec![Some("hi".to_string()), Some("bye".to_string())]
    );
    // TODO: column "middle" (a struct)
    // TODO: column "list"
    // TODO: column "map"
}
