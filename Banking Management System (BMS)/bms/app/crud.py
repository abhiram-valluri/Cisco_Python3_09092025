from sqlalchemy.exc import IntegrityError
from .db import get_db
from .models import Account
from .exceptions import NotFoundError, ValidationError


def create_account(name: str, number: str, balance: float) -> Account:
    db = get_db()
    account = Account(name=name, number=number, balance=float(balance))
    db.add(account)
    try:
        db.commit()
        db.refresh(account)
    except IntegrityError as exc:
        db.rollback()
        raise ValidationError(f"Account number must be unique: {number}")
    return account


def get_account(account_id: int) -> Account:
    db = get_db()
    acc = db.query(Account).filter(Account.id == account_id).first()
    if not acc:
        raise NotFoundError(f"Account id={account_id} not found")
    return acc


def list_accounts(offset: int = 0, limit: int = 100):
    db = get_db()
    q = db.query(Account).offset(offset).limit(limit).all()
    return q


def update_account(account_id: int, **fields) -> Account:
    db = get_db()
    acc = db.query(Account).filter(Account.id == account_id).first()
    if not acc:
        raise NotFoundError(f"Account id={account_id} not found")
    for k, v in fields.items():
        if hasattr(acc, k):
            setattr(acc, k, v)
    db.add(acc)
    db.commit()
    db.refresh(acc)
    return acc


def delete_account(account_id: int):
    db = get_db()
    acc = db.query(Account).filter(Account.id == account_id).first()
    if not acc:
        raise NotFoundError(f"Account id={account_id} not found")
    db.delete(acc)
    db.commit()
    return True
