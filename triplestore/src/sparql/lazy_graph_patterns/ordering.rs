use spargebra::algebra::GraphPattern;

pub enum Order {
    Left,
    Right,
}

pub fn decide_order(left: &GraphPattern, right: &GraphPattern) -> Order {
    if let GraphPattern::Path { .. } = left {
        Order::Right
    } else if let GraphPattern::Path { .. } = right {
        Order::Left
    } else {
        Order::Left
    }
}
