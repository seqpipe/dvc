from dvc.logger import Logger
from dvc.cli import parse_args

def main(argv=None):
    Logger.init()

    try:
        import setproctitle  # @UnresolvedImport
        setproctitle.setproctitle('dvc')
    except ImportError:
        pass

    args = parse_args(argv)

    try:
        cmd = args.func(args)
    except Exception as ex:
        Logger.error('Initialization error', ex)
        return 255

    try:
        ret = cmd.run_cmd()
    except Exception as ex:
        Logger.error('Unexpected error', ex)
        return 254

    return ret
