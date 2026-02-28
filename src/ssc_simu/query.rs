Ast {
    expressions: [
        Expr(
            Select(
                SelectStatement {
                    fields: Select(
                        [
                            All,
                            Single(
                                Selector {
                                    expr: Idiom(
                                        Idiom(
                                            [
                                                Start(
                                                    Select(
                                                        SelectStatement {
                                                            fields: Select(
                                                                [
                                                                    All,
                                                                ],
                                                            ),
                                                            omit: [],
                                                            only: false,
                                                            what: [
                                                                Table(
                                                                    "user",
                                                                ),
                                                            ],
                                                            with: None,
                                                            cond: Some(
                                                                Cond(
                                                                    Binary {
                                                                        left: Idiom(
                                                                            Idiom(
                                                                                [
                                                                                    Field(
                                                                                        "id",
                                                                                    ),
                                                                                ],
                                                                            ),
                                                                        ),
                                                                        op: Equal,
                                                                        right: Idiom(
                                                                            Idiom(
                                                                                [
                                                                                    Start(
                                                                                        Param(
                                                                                            Param(
                                                                                                "parent",
                                                                                            ),
                                                                                        ),
                                                                                    ),
                                                                                    Field(
                                                                                        "author",
                                                                                    ),
                                                                                ],
                                                                            ),
                                                                        ),
                                                                    },
                                                                ),
                                                            ),
                                                            split: None,
                                                            group: None,
                                                            order: None,
                                                            limit: Some(
                                                                Limit(
                                                                    Literal(
                                                                        Integer(
                                                                            1,
                                                                        ),
                                                                    ),
                                                                ),
                                                            ),
                                                            start: None,
                                                            fetch: None,
                                                            version: Literal(
                                                                None,
                                                            ),
                                                            timeout: Literal(
                                                                None,
                                                            ),
                                                            explain: None,
                                                            tempfiles: false,
                                                        },
                                                    ),
                                                ),
                                                Value(
                                                    Literal(
                                                        Integer(
                                                            0,
                                                        ),
                                                    ),
                                                ),
                                            ],
                                        ),
                                    ),
                                    alias: Some(
                                        Idiom(
                                            [
                                                Field(
                                                    "author",
                                                ),
                                            ],
                                        ),
                                    ),
                                },
                            ),
                            Single(
                                Selector {
                                    expr: Select(
                                        SelectStatement {
                                            fields: Select(
                                                [
                                                    All,
                                                    Single(
                                                        Selector {
                                                            expr: Idiom(
                                                                Idiom(
                                                                    [
                                                                        Start(
                                                                            Select(
                                                                                SelectStatement {
                                                                                    fields: Select(
                                                                                        [
                                                                                            All,
                                                                                        ],
                                                                                    ),
                                                                                    omit: [],
                                                                                    only: false,
                                                                                    what: [
                                                                                        Table(
                                                                                            "user",
                                                                                        ),
                                                                                    ],
                                                                                    with: None,
                                                                                    cond: Some(
                                                                                        Cond(
                                                                                            Binary {
                                                                                                left: Idiom(
                                                                                                    Idiom(
                                                                                                        [
                                                                                                            Field(
                                                                                                                "id",
                                                                                                            ),
                                                                                                        ],
                                                                                                    ),
                                                                                                ),
                                                                                                op: Equal,
                                                                                                right: Idiom(
                                                                                                    Idiom(
                                                                                                        [
                                                                                                            Start(
                                                                                                                Param(
                                                                                                                    Param(
                                                                                                                        "parent",
                                                                                                                    ),
                                                                                                                ),
                                                                                                            ),
                                                                                                            Field(
                                                                                                                "author",
                                                                                                            ),
                                                                                                        ],
                                                                                                    ),
                                                                                                ),
                                                                                            },
                                                                                        ),
                                                                                    ),
                                                                                    split: None,
                                                                                    group: None,
                                                                                    order: None,
                                                                                    limit: Some(
                                                                                        Limit(
                                                                                            Literal(
                                                                                                Integer(
                                                                                                    1,
                                                                                                ),
                                                                                            ),
                                                                                        ),
                                                                                    ),
                                                                                    start: None,
                                                                                    fetch: None,
                                                                                    version: Literal(
                                                                                        None,
                                                                                    ),
                                                                                    timeout: Literal(
                                                                                        None,
                                                                                    ),
                                                                                    explain: None,
                                                                                    tempfiles: false,
                                                                                },
                                                                            ),
                                                                        ),
                                                                        Value(
                                                                            Literal(
                                                                                Integer(
                                                                                    0,
                                                                                ),
                                                                            ),
                                                                        ),
                                                                    ],
                                                                ),
                                                            ),
                                                            alias: Some(
                                                                Idiom(
                                                                    [
                                                                        Field(
                                                                            "author",
                                                                        ),
                                                                    ],
                                                                ),
                                                            ),
                                                        },
                                                    ),
                                                ],
                                            ),
                                            omit: [],
                                            only: false,
                                            what: [
                                                Table(
                                                    "comment",
                                                ),
                                            ],
                                            with: None,
                                            cond: Some(
                                                Cond(
                                                    Binary {
                                                        left: Idiom(
                                                            Idiom(
                                                                [
                                                                    Field(
                                                                        "thread",
                                                                    ),
                                                                ],
                                                            ),
                                                        ),
                                                        op: Equal,
                                                        right: Idiom(
                                                            Idiom(
                                                                [
                                                                    Start(
                                                                        Param(
                                                                            Param(
                                                                                "parent",
                                                                            ),
                                                                        ),
                                                                    ),
                                                                    Field(
                                                                        "id",
                                                                    ),
                                                                ],
                                                            ),
                                                        ),
                                                    },
                                                ),
                                            ),
                                            split: None,
                                            group: None,
                                            order: Some(
                                                Order(
                                                    OrderList(
                                                        [
                                                            Order {
                                                                value: Idiom(
                                                                    [
                                                                        Field(
                                                                            "created_at",
                                                                        ),
                                                                    ],
                                                                ),
                                                                collate: false,
                                                                numeric: false,
                                                                direction: false,
                                                            },
                                                        ],
                                                    ),
                                                ),
                                            ),
                                            limit: Some(
                                                Limit(
                                                    Literal(
                                                        Integer(
                                                            10,
                                                        ),
                                                    ),
                                                ),
                                            ),
                                            start: None,
                                            fetch: None,
                                            version: Literal(
                                                None,
                                            ),
                                            timeout: Literal(
                                                None,
                                            ),
                                            explain: None,
                                            tempfiles: false,
                                        },
                                    ),
                                    alias: Some(
                                        Idiom(
                                            [
                                                Field(
                                                    "comments",
                                                ),
                                            ],
                                        ),
                                    ),
                                },
                            ),
                        ],
                    ),
                    omit: [],
                    only: false,
                    what: [
                        Table(
                            "thread",
                        ),
                    ],
                    with: None,
                    cond: Some(
                        Cond(
                            Binary {
                                left: Idiom(
                                    Idiom(
                                        [
                                            Field(
                                                "id",
                                            ),
                                        ],
                                    ),
                                ),
                                op: Equal,
                                right: Param(
                                    Param(
                                        "id",
                                    ),
                                ),
                            },
                        ),
                    ),
                    split: None,
                    group: None,
                    order: None,
                    limit: Some(
                        Limit(
                            Literal(
                                Integer(
                                    1,
                                ),
                            ),
                        ),
                    ),
                    start: None,
                    fetch: None,
                    version: Literal(
                        None,
                    ),
                    timeout: Literal(
                        None,
                    ),
                    explain: None,
                    tempfiles: false,
                },
            ),
        ),
    ]
}
