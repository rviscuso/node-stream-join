/**
 * Created by rviscuso on 7/12/17.
 */

const j = require('../index');
const assert = require('assert');

const a1 = [
    {name: 'Cosmo', ss: 12345},
    {name: 'Kramer', ss: 12345},
    {name: 'Kramer', ss: 11111},
    {name: 'Elaine', ss: 23456},
    {name: 'George', ss: 34567},
    {name: 'Newman', ss: 45678},
    {name: 'Jerry', ss: 56789},
];
const a2 = [
    {ssn: 12345, name: 'Kramer', score: 500},
    {ssn: 23456, name: 'Elaine', score: 600},
    {ssn: 34567, name: 'George', score: 700},
    {ssn: 45678, name: 'Newman', score: 800},
    {ssn: 56789, name: 'Jerry', score: 750},
];
const a3 = [
    {social: 12345, dl: 11111},
    {social: 23456, dl: 22222},
    {social: 34567, dl: 33333},
    {social: 45678, dl: 44444},
    {social: 56789, dl: 55555},
];

const s1 = () => j.array2Stream(a1);
const s2 = () => j.array2Stream(a2);
let s3 = () => j.array2Stream(a3);

describe('index', function () {

    it('join on same field name', async function () {
        let joined = j.join([s1(), s2()], [['name', 'name']]);
        joined.on('data', x => console.log(x));
        joined.on('error', err => done(err));
        joined.on('end', () => done());
    });

    it('join on different field names', async function () {
        let joined = j.join([s1(), s2(), s3()], [['ss', 'ssn', 'social']]);
        joined.on('data', x => console.log(x));
        joined.on('error', err => done(err));
        joined.on('end', () => done());
    });

    it('join on 2 different fields', async function () {
        let joined = j.join([s1(), s2()], [['ss', 'ssn'], ['name', 'name']]);
        joined.on('data', x => console.log(x));
        joined.on('error', err => done(err));
        joined.on('end', () => done());
    });

    it('join streams only', async function () {
        let joined = j.join([s1(), s2()], [['ss', 'ssn']]);
        joined.on('data', x => console.log(x));
        joined.on('error', err => done(err));
        joined.on('end', () => done());
    });

    it('join streams with extended info', async function () {
        let joined = j.join(
            [{stream: s1(), nullable:true, includeJoinField: true}, {stream: s2(), nullable:true}],
            [['ss', 'ssn']]);

        joined.on('data', x => console.log(x));
        joined.on('error', err => done(err));
        joined.on('end', () => done());
    })

});