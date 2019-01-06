module.exports = {
    squash(array) {
        return array.reduce(
            (m, e) => {
                Object.keys(e).forEach(k => {
                    if (m[k]) {
                        if (!Array.isArray(m[k]) && m[k] !== e[k]) m[k] = [m[k]];
                        if (m[k].indexOf && m[k].indexOf(e[k]) < 0) m[k].push(e[k]);
                    }
                    else {
                        m[k] = e[k];
                    }
                });
                return m;
            },
            {});
    },

    cartesian(arrays) {
        return arrays.reduce((a, b) => {
            if (!Array.isArray(b)) b = [b];
            return module.exports.flatten(a.map((x) => {
                return b.map((y) => {
                    return x.concat([y]);
                });
            }), true);
        }, [[]]);
    },

    flatten(array) {
        return array.reduce((m, c) => m.concat(c), [])
    }
}