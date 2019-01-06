const { squash, flatten, cartesian } = require('../array-utils');
const { should } = require('chai');

describe('array-utils', function () {

    it('array squash', function () {
        const a = [
            { ss: 11111 },
            { ss: 12345, ssn: 12345, score: 500 },
            { ss: 12345, ssn: 12345, score: 500 },
        ]

        squash(a).should.eql({ ss: [11111, 12345], ssn: 12345, score: 500 })
    })

    it('array flatten', function () {
        const a = [[1, 2], [3, 4], 5, 6, [7, 8, 9], null]
        flatten(a).should.eql([1, 2, 3, 4, 5, 6, 7, 8, 9, null])
    })

    it('array cartesian', function () {
        const a = [
            [
                { a: 1, b: 2 },
                { a: 2, b: 3, c: 4 }
            ],
            [
                { d: 7, e: 8 },
                { f: 9 }
            ]
        ]

        cartesian(a).should.eql([
            [{ a: 1, b: 2 }, { d: 7, e: 8 }],
            [{ a: 1, b: 2 }, { f: 9 }],
            [{ a: 2, b: 3, c: 4 }, { d: 7, e: 8 }],
            [{ a: 2, b: 3, c: 4 }, { f: 9 }]
        ])
    })
})