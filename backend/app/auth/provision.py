"""Interactive one-time bootstrap; never accepts passwords on command lines."""
from getpass import getpass
from app.auth.service import create_user
from app.db.session import SessionLocal


def main():
    name = input('Initial administrator username: ')
    password = getpass('Password (15–128 characters): ')
    if password != getpass('Confirm password: '):
        raise SystemExit('Passwords do not match')
    try:
        with SessionLocal() as db, db.begin():
            create_user(db, name, password, 'administrator', initial=True)
    except Exception:
        raise SystemExit('Provisioning failed. Check migration, username/password policy, and existing bootstrap state.')
    print('Initial administrator provisioned. Sign in through the application.')


if __name__ == '__main__':
    main()
