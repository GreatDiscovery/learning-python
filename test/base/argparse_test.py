import argparse


# python argparse_test.py 1 2 3 --sum

def cmd1Get(args):
    print("cmd1 get")
    print(args)


def cmd2Set(args):
    print("cmd2 set")
    print(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest="subcmd", title="subcmd")

    cmd1 = subparser.add_parser("cmd1", help="cmd1 help")
    cmd2 = subparser.add_parser("cmd2", help="cmd2 help")

    sub_subparser = cmd1.add_subparsers(dest="subsubcmd", title="subsubcmd")
    get = sub_subparser.add_parser("get", help="op get")
    get.set_defaults(func=cmd1Get)

    sub_subparser = cmd2.add_subparsers(dest="subsubcmd", title="subsubcmd")
    set = sub_subparser.add_parser("set", help="op set")
    set.set_defaults(func=cmd2Set)

    get.add_argument("--id", help="ID", type=str, required=True)
    set.add_argument("--id", help="ID", type=str, required=True)

    args = parser.parse_args()

    if not args.subcmd:
        parser.print_help()
    elif not args.subsubcmd:
        subparser.choices[args.subcmd].print_help()  # 输出子命令的帮助
    else:
        args.func(args)
