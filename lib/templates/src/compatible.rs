// pub fn validate_compatible_ptypes(argument_ptype:&PType, signature_ptype:&PType) -> bool {
//     match argument_ptype {
//         PType::None => {
//             true
//         }
//         PType::Basic(argument_basic) => {
//             match signature_ptype {
//                 PType::None => true,
//                 PType::Basic(signature_basic) => {
//                     if argument_basic.as_str() == OTTR_IRI {
//
//                     }
//                 }
//                 PType::Lub(_) => {}
//                 PType::List(_) => {}
//                 PType::NEList(_) => {}
//             }
//         }
//         PType::Lub(argument_inner) | PType::List(argument_inner) | PType::NEList(argument_inner) => {
//
//         }
//     }
// }
