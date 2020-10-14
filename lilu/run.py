import os
import json
import click
import urllib
import requests
import webbrowser

from hashlib import sha256
from datetime import datetime, timedelta
from typing import Optional
from moda.user import UserInteractor
from data_layer.encryptor import Encryptor
from cryptography.hazmat.backends import default_backend

class Lilu:
  user: UserInteractor

  def __init__(self, interactive: bool):
    self.user = UserInteractor(
      timeout=None,
      interactive=interactive
    )

@click.group()
@click.option('--use-the-force/--no-use-the-force', 'use_the_force', is_flag=True)
@click.pass_context
def run(ctx: any, use_the_force: bool):
  ctx.obj = Lilu(
    interactive=not use_the_force
  )

@run.group()
def authorize():
  pass

@authorize.command()
@click.option('-a', '--app-id', 'app_id', prompt=True)
@click.pass_obj
def code(lilu: Lilu, app_id: str):
  if not lilu.user.present_confirmation('Please log in to TikTok with the credentials of the account you wish to authorize or a management account with access to it', default_response=True):
    raise click.Abort()
  url = f'https://ads.tiktok.com/marketing_api/auth?app_id={app_id}&redirect_uri=https%3A%2F%2Fhello.xyla.io'
  webbrowser.open(url)
  lilu.user.present_message('Your authorization code is the value of the \'auth_code\' query parameter in the URL to which you are redirected after confirming the authorization in the TikTok web interface.')

@authorize.command()
@click.option('-a', '--app-id', 'app_id', prompt=True)
@click.option('-n', '--name', 'name', prompt=True)
@click.option('-e', '--expire-days', 'expire_days', type=int, default=90)
@click.option('-r', '--redirect-url', 'redirect_url', default='https://hello.xyla.io')
@click.option('-c', '--cipher-key', 'cipher_key')
@click.option('-v', '--initialization-vector', 'initialization_vector')
@click.pass_obj
def code_link(lilu: Lilu, app_id: str, name: str, expire_days: int, redirect_url: str, cipher_key: Optional[str], initialization_vector: Optional[str]):
  if cipher_key is None:
    cipher_key = os.urandom(32).hex()
  if initialization_vector is None:
    initialization_vector = os.urandom(16).hex()
  now = datetime.utcnow()
  expire = now + timedelta(days=expire_days)
  state = {
    'app_id': app_id,
    'name': name,
    'time': datetime.timestamp(now),
    'expire': datetime.timestamp(expire),
  }
  enciphered_state = Encryptor.encipher(
    data=json.dumps(state).encode(),
    key=bytes.fromhex(cipher_key),
    initialization_vector=bytes.fromhex(initialization_vector),
    backend=default_backend()
  )
  enciphered_state_hex = enciphered_state.hex()
  enciphered_hash = sha256()
  enciphered_hash.update(enciphered_state_hex.encode())
  enciphered_hash.update(initialization_vector.encode())
  enciphered_hash_hex = enciphered_hash.digest().hex()
  redirect_url_escaped = urllib.parse.quote_plus(redirect_url)
  url = f'https://ads.tiktok.com/marketing_api/auth?app_id={app_id}&state={enciphered_state_hex}{enciphered_hash_hex}&redirect_uri={redirect_url_escaped}'
  lilu.user.present_message(f'app_id:\n{app_id}\nname:\n{name}\nexpire:\n{expire}\ncipher_key:\n{cipher_key}\ninitialization_vector:\n{initialization_vector}\nredirect_url:\n{redirect_url}')
  lilu.user.present_message(f'Please ask the client to log in to TikTok with the credentials of the account you wish to authorize or a management account with access to it, then follow the link below:\n{url}')

@authorize.command()
@click.option('-a', '--app-id', 'app_id', prompt=True)
@click.option('-s', '--secret', 'secret', prompt=True, hide_input=True)
@click.option('-c', '--code', 'code', prompt=True, hide_input=True)
@click.pass_obj
def long_access_token(lilu: Lilu, app_id: str, secret: str, code: str):
  payload = {
    'app_id': app_id,
    'secret': secret,
    'auth_code': code,
  }
  response = requests.post('https://ads.tiktok.com/open_api/oauth2/access_token_v2/', json=payload)
  response_json = response.json()['data']
  linebreak = '\n'
  lilu.user.present_message(f'Your long term access token is:\n{response_json["access_token"]}\nAuthorized_advertiser IDs:\n{linebreak.join(map(str, response_json["advertiser_ids"]))}\nAuthorized scope:\n{", ".join(map(str, response_json["scope"]))}')
  return response_json

@authorize.command()
@click.option('-a', '--app-id', 'app_id', prompt=True)
@click.option('-s', '--secret', 'secret', prompt=True, hide_input=True)
@click.option('-c', '--code', 'code', prompt=True, hide_input=True)
@click.pass_obj
def refresh_token(lilu: Lilu, app_id: str, secret: str, code: str):
  payload = {
    'app_id': app_id,
    'secret': secret,
    'grant_type': 'auth_code',
    'auth_code': code,
  }
  response = requests.post('https://ads.tiktok.com/open_api/oauth2/access_token/', json=payload)
  response_json = response.json()['data']
  lilu.user.present_message(f'Your access token is:\n{response_json["access_token"]}\n(expires in {timedelta(seconds=response_json["expires_in"])})\nYour refresh token is:\n{response_json["refresh_token"]}\n(expires in {timedelta(seconds=response_json["refresh_token_expires_in"])})')
  return response_json

@authorize.command()
@click.option('-a', '--app-id', 'app_id', prompt=True)
@click.option('-s', '--secret', 'secret', prompt=True, hide_input=True)
@click.option('-r', '--refresh-token', 'refresh_token', prompt=True, hide_input=True)
@click.pass_obj
def access_token(lilu: Lilu, app_id: str, secret: str, refresh_token: str):
  payload = {
    'app_id': app_id,
    'secret': secret,
    'grant_type': 'refresh_token',
    'refresh_token': refresh_token, 
  }
  response = requests.post('https://ads.tiktok.com/open_api/oauth2/refresh_token/', json=payload)
  response_json = response.json()['data']
  lilu.user.present_message(f'Your access token is:\n{response_json["access_token"]}\n(expires in {timedelta(seconds=response_json["expires_in"])})\nYour refresh token is:\n{response_json["refresh_token"]}\n(expires in {timedelta(seconds=response_json["refresh_token_expires_in"])})')
  return response_json
