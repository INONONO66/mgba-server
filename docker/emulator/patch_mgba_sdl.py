from pathlib import Path

path = Path('/mgba/src/platform/sdl/main.c')
text = path.read_text()


def replace_once(haystack: str, needle: str, replacement: str) -> str:
    count = haystack.count(needle)
    if count != 1:
        raise RuntimeError(f'Expected exactly one match for patch anchor, found {count}: {needle[:80]!r}')
    return haystack.replace(needle, replacement, 1)

text = replace_once(
    text,
    '#include <mgba/core/scripting.h>\n',
    '#include <mgba/core/scripting.h>\n#include <mgba/script/base.h>\n#include <mgba/script/console.h>\n#include <mgba/script/context.h>\n',
)
text = replace_once(
    text,
    '#include <signal.h>\n',
    '#include <signal.h>\n#include <string.h>\n',
)
text = replace_once(
    text,
    'static struct VFile* _state = NULL;\n',
    '''static struct VFile* _state = NULL;

#ifdef ENABLE_SCRIPTING
struct mScriptOpts {
\tconst char* script;
};

static const char* _scriptPath = NULL;

static const struct mOption _scriptLongOpts[] = {
\t{ "script", true, '\\0' },
\t{ 0, 0, 0 }
};

static bool _parseLongScriptArg(struct mSubParser* parser, const char* option, const char* arg) {
\tstruct mScriptOpts* opts = parser->opts;
\tif (strcmp(option, "script") == 0) {
\t\topts->script = arg;
\t\treturn true;
\t}
\treturn false;
}

static void mSubParserScriptInit(struct mSubParser* parser, struct mScriptOpts* opts) {
\tparser->usage = "Scripting options:\\n  --script FILE              Load Lua script";
\tparser->opts = opts;
\tparser->parse = NULL;
\tparser->parseLong = _parseLongScriptArg;
\tparser->apply = NULL;
\tparser->extraOptions = NULL;
\tparser->longOptions = _scriptLongOpts;
\topts->script = NULL;
}
#endif
''',
)
text = replace_once(
    text,
    '''\tstruct mSubParser subparser;

\tmSubParserGraphicsInit(&subparser, &graphicsOpts);
\tbool parsed = mArgumentsParse(&args, argc, argv, &subparser, 1);
''',
    '''\tstruct mSubParser subparsers[2];
\tint nSubparsers = 1;
#ifdef ENABLE_SCRIPTING
\tstruct mScriptOpts scriptOpts;
#endif

\tmSubParserGraphicsInit(&subparsers[0], &graphicsOpts);
#ifdef ENABLE_SCRIPTING
\tmSubParserScriptInit(&subparsers[1], &scriptOpts);
\tnSubparsers = 2;
#endif
\tbool parsed = mArgumentsParse(&args, argc, argv, subparsers, nSubparsers);
''',
)
text = replace_once(text, 'usage(argv[0], NULL, NULL, &subparser, 1);', 'usage(argv[0], NULL, NULL, subparsers, nSubparsers);')
text = replace_once(text, 'mArgumentsApply(&args, &subparser, 1, &renderer.core->config);', 'mArgumentsApply(&args, subparsers, nSubparsers, &renderer.core->config);')
text = replace_once(
    text,
    '''\tif (args.showVersion) {
\t\tversion(argv[0]);
\t\tmArgumentsDeinit(&args);
\t\treturn 0;
\t}
''',
    '''\tif (args.showVersion) {
\t\tversion(argv[0]);
\t\tmArgumentsDeinit(&args);
\t\treturn 0;
\t}
#ifdef ENABLE_SCRIPTING
\t_scriptPath = scriptOpts.script;
#endif
''',
)
text = replace_once(
    text,
    '''\tmCoreAutoloadSave(renderer->core);
\tmArgumentsApplyFileLoads(args, renderer->core);
#ifdef ENABLE_SCRIPTING
\tstruct mScriptBridge* bridge = mScriptBridgeCreate();
''',
    '''\tmCoreAutoloadSave(renderer->core);
\tmArgumentsApplyFileLoads(args, renderer->core);
#ifdef ENABLE_SCRIPTING
\tstruct mScriptContext scriptContext;
\tmScriptContextInit(&scriptContext);
\tmScriptContextAttachStdlib(&scriptContext);
\tmScriptContextAttachSocket(&scriptContext);
\tmScriptContextRegisterEngines(&scriptContext);
\tmScriptContextAttachLogger(&scriptContext, &_logger.d);
\tthread.scriptContext = &scriptContext;

\tstruct mScriptBridge* bridge = mScriptBridgeCreate();
''',
)
text = replace_once(
    text,
    '''\tbool didFail = !mCoreThreadStart(&thread);

\tif (!didFail) {
''',
    '''\tbool didFail = !mCoreThreadStart(&thread);

#ifdef ENABLE_SCRIPTING
\tif (!didFail && _scriptPath) {
\t\tmCoreThreadPause(&thread);
\t\tif (!mScriptContextLoadFile(&scriptContext, _scriptPath)) {
\t\t\tprintf("Could not load script: %s\\n", _scriptPath);
\t\t}
\t\tmCoreThreadUnpause(&thread);
\t}
#endif

\tif (!didFail) {
''',
)
text = replace_once(
    text,
    '''#ifdef ENABLE_SCRIPTING
\tmScriptBridgeDestroy(bridge);
#endif
''',
    '''#ifdef ENABLE_SCRIPTING
\tmScriptBridgeDestroy(bridge);
\tmScriptContextDeinit(&scriptContext);
#endif
''',
)

if 'parseLongScriptArg' not in text or 'mScriptContextLoadFile' not in text:
    raise RuntimeError('mGBA SDL script patch did not apply')

path.write_text(text)
