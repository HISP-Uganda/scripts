const webpack = require('webpack');
const path = require('path');
const fs = require('fs');

let nodeModules = {};
fs.readdirSync('node_modules')
    .filter(function (x) {
        return ['.bin'].indexOf(x) === -1;
    })
    .forEach(function (mod) {
        nodeModules[mod] = 'commonjs ' + mod;
    });

module.exports = {
    entry: './src/main.js',
    target: 'node',
    output: {
        path: __dirname,
        filename: 'backend.js'
    },
    externals: nodeModules
};
