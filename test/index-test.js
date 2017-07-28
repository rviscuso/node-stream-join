/**
 * Created by rviscuso on 7/12/17.
 */

require('chai').should();
const _ = require('highland');

const j = require('../index');

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
    {ssn: 99999, name: 'Frank', score: 750},
];

const a3 = [
    {social: 12345, dl: 11111},
    {social: 23456, dl: 22222},
    {social: 34567, dl: 33333},
    {social: 45678, dl: 44444},
    {social: 56789, dl: 55555},
];

let s1, s2, s3;

describe('index', function () {

    beforeEach(function(){
        s1 = _(a1);
        s2 = _(a2);
        s3 = _(a3);
    });

    it('join on same field name, no squash', function () {
        return _(j.join([s1, s2], [['name', 'name']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(6);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on same field name, yes squash', function () {
        return _(j.join([{stream: s1, squash: true}, {stream: s2}], [['name', 'name']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                console.log(r);
                r.length.should.equal(5);
                r.map(x => x.name).should.eql(['Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on different field names', async function () {
        return _(j.join([s1, s2, s3], [['ss', 'ssn', 'social']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                console.log(r);
                r.length.should.equal(6);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on 2 different fields', async function () {
        return _(j.join([s1, s2], [['ss', 'ssn'], ['name', 'name']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                console.log(r);
                r.length.should.equal(5);
                r.map(x => x.name).should.eql(['Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join streams with nullable true', async function () {
        return _(j.join(
            [{stream: s1, nullable:true, includeJoinField: true}, {stream: s2, nullable:true}],
            [['ss', 'ssn']])
        )
            .collect()
            .toPromise(Promise)
            .then(r => {
                console.log(r);
                r.length.should.equal(8);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer','Kramer', 'Elaine', 'George', 'Newman', 'Jerry', 'Frank']);
                // Note Cosmo gets overwritten by Kramer since they have the same SSN, so this is correct
            });
    });

    it('join streams with aliases', async function () {
        return _(j.join(
            [{stream: s1, alias: 'ssnData'}, {stream: s2, alias: 'creditData'}],
            [['ss', 'ssn']])
        )
            .collect()
            .toPromise(Promise)
            .then(r => {
                console.log(r);
                r.length.should.equal(6);
                r.forEach(x => x.should.include.keys(['ssnData', 'creditData']));
            });
    })

});