log('info', 'exec today...');

if (shell.which('node')) {
    log('info', 'node ./bin/jrjfund_format.js');
    shell.exec("node ./bin/jrjfund_format.js");

    log('info', 'node ./bin/jrjfundma.js');
    shell.exec("node ./bin/jrjfundma.js");

    log('info', 'node ./bin/jrjfundnop.js');
    shell.exec("node ./bin/jrjfundnop.js");

    log('info', 'node ./bin/jrjfundnopr.js');
    shell.exec("node ./bin/jrjfundnopr.js");

    log('info', 'node ./bin/jrjfundrsi.js');
    shell.exec("node ./bin/jrjfundrsi.js");

    log('info', 'node ./bin/jrjfundema.js');
    shell.exec("node ./bin/jrjfundema.js");
}

log('info', 'exec today end.');