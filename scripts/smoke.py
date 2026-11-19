#!/usr/bin/env python3
"""Exercise real Auth -> Media -> Submission -> Moderation, stopping at APPROVED.

Requires running services and dedicated existing student, other-student, and
moderator accounts. Never grants roles, signs tokens, publishes catalog data,
installs mocks, or resets shared state. Credentials stay in process memory.
"""
import json
import os
import struct
import time
import urllib.error
import sys
import urllib.request
import uuid
import zlib

AUTH = os.environ.get('SD_SMOKE_AUTH_URL', 'http://localhost:8080').rstrip('/')
MEDIA = os.environ.get('SD_SMOKE_MEDIA_URL', 'http://localhost:8083').rstrip('/')
SUBMISSION = os.environ.get('SD_SMOKE_SUBMISSION_URL', 'http://localhost:8081').rstrip('/')
MODERATION = os.environ.get('SD_SMOKE_MODERATION_URL', 'http://localhost:8082').rstrip('/')
RUN = uuid.uuid4().hex


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


HTTP = urllib.request.build_opener(NoRedirect())


def request(base, path, token=None, method='GET', body=None, headers=None, expected=(200,)):
    actual = {'X-Correlation-ID': 'smoke-' + RUN}
    if token:
        actual['Authorization'] = 'Bearer ' + token
    if body is not None and not isinstance(body, bytes):
        body = json.dumps(body).encode()
        actual['Content-Type'] = 'application/json'
    actual.update(headers or {})
    req = urllib.request.Request(base + path, data=body, headers=actual, method=method)
    try:
        response = HTTP.open(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = response.read()
        if response.status not in expected:
            # Never dump responses that could contain credentials/private payloads.
            raise AssertionError(f'{method} {path}: expected {expected}, got {response.status}')
        if 'json' in response.headers.get('Content-Type', '') and data:
            data = json.loads(data)
        return data, response.headers


def eventually(description, action, predicate):
    deadline = time.monotonic() + 45
    while True:
        value = action()
        if predicate(value):
            return value
        if time.monotonic() >= deadline:
            raise AssertionError('Timed out: ' + description)
        time.sleep(0.25)


def png():
    def chunk(kind, content):
        return struct.pack('!I', len(content)) + kind + content + struct.pack('!I', zlib.crc32(kind + content))
    return (b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('!2I5B', 2, 2, 8, 2, 0, 0, 0))
            + chunk(b'IDAT', zlib.compress(b'\x00\xff\x00\x00\x00\xff\x00' * 2)) + chunk(b'IEND', b''))


def main():
    tokens = {}
    try:
        for role in ('STUDENT', 'OTHER', 'MODERATOR'):
            credentials = {
                'email': os.environ['SD_SMOKE_' + role + '_EMAIL'],
                'password': os.environ['SD_SMOKE_' + role + '_PASSWORD'],
            }
            login, _ = request(AUTH, '/api/v1/auth/login', method='POST', body=credentials)
            tokens[role] = login['accessToken']
        student, other, moderator = (tokens[role] for role in ('STUDENT', 'OTHER', 'MODERATOR'))
        request(MODERATION, '/api/v1/moderation/queue', expected=(401,))
        request(MODERATION, '/api/v1/moderation/queue', student, expected=(403,))
        request(MEDIA, '/media/v1/assets/' + str(uuid.uuid4()), expected=(401,))
        boundary = 'smoke-' + RUN
        image = png()
        upload = (('--' + boundary + '\r\nContent-Disposition: form-data; name="file"; filename="proof.png"\r\n'
                   'Content-Type: image/png\r\n\r\n').encode() + image + ('\r\n--' + boundary + '--\r\n').encode())
        upload_headers = {'Content-Type': 'multipart/form-data; boundary=' + boundary, 'Idempotency-Key': RUN + '-upload'}
        asset, _ = request(MEDIA, '/api/v1/media-assets', student, 'POST', upload, upload_headers, (202,))
        asset_id = asset['id']
        replay, _ = request(MEDIA, '/api/v1/media-assets', student, 'POST', upload, upload_headers, (200, 202))
        assert replay['id'] == asset_id, 'Upload replay created duplicate asset'
        asset_path = '/api/v1/media-assets/' + asset_id
        eventually('media CLEAN', lambda: request(MEDIA, asset_path, student)[0], lambda a: a['status'] == 'CLEAN')
        preview, preview_headers = request(MEDIA, asset_path + '/preview', student)
        assert preview.startswith(b'\x89PNG\r\n\x1a\n')
        assert preview_headers['X-Content-Type-Options'] == 'nosniff'
        request(MEDIA, asset_path, other, expected=(404,))
        request(MEDIA, asset_path + '/preview', other, expected=(404,))
        payload = {
            'place': {'name': 'Smoke ' + RUN, 'city': 'Санкт-Петербург',
                      'addressLine': 'Кронверкский пр., 49', 'categoryCode': 'cafe',
                      'location': {'lat': 59.9571, 'lon': 30.3083}},
            'offers': [{'conditions': 'Student smoke discount', 'discountType': 'PERCENT', 'discountValue': 10}],
            'attachments': [{'mediaAssetId': asset_id, 'purpose': 'PROOF', 'offerIndex': 0}],
        }
        created, headers = request(SUBMISSION, '/api/v1/submissions', student, 'POST',
                                   {'type': 'NEW_PLACE', 'payload': payload}, {'Idempotency-Key': RUN + '-create'}, (201,))
        submission_id = created['id']
        submission_path = '/api/v1/submissions/' + submission_id
        request(SUBMISSION, submission_path + '/submit', student, 'POST', headers={
            'If-Match': headers['ETag'], 'Idempotency-Key': RUN + '-submit'}, expected=(202,))
        eventually('Submission PENDING_REVIEW', lambda: request(SUBMISSION, submission_path, student)[0],
                   lambda s: s['status'] == 'PENDING_REVIEW')
        review_path = '/api/v1/moderation/reviews/' + submission_id
        eventually('moderation queue projection', lambda: request(MODERATION, '/api/v1/moderation/queue?limit=100', moderator)[0],
                   lambda queue: any(item['submissionId'] == submission_id for item in queue))
        review, headers = request(MODERATION, review_path, moderator)
        claim = {'action': 'CLAIM', 'versionId': review['versionId'], 'versionNo': review['versionNo']}
        claim_headers = {'If-Match': headers['ETag'], 'Idempotency-Key': RUN + '-claim'}
        command, _ = request(MODERATION, review_path + '/commands', moderator, 'POST', claim, claim_headers, (202,))
        repeated, _ = request(MODERATION, review_path + '/commands', moderator, 'POST', claim, claim_headers, (202,))
        assert repeated['commandId'] == command['commandId'], 'Claim replay duplicated command'
        eventually('confirmed IN_REVIEW', lambda: request(MODERATION, review_path, moderator)[0],
                   lambda r: r['status'] == 'IN_REVIEW' and r['pendingCommandId'] is None)
        submitted, submission_headers = request(SUBMISSION, submission_path, moderator)
        assert submitted['status'] == 'IN_REVIEW'
        payload['offers'][0]['conditions'] = 'Corrected student smoke discount'
        request(SUBMISSION, submission_path + '/versions', moderator, 'POST', {
            'reviewedVersionId': claim['versionId'], 'payload': payload,
        }, {'If-Match': submission_headers['ETag'], 'Idempotency-Key': RUN + '-correct'}, (201,))
        eventually('corrected immutable version releases prior assignment',
                   lambda: request(MODERATION, review_path, moderator)[0],
                   lambda r: r['versionNo'] == claim['versionNo'] + 1 and r['reviewerId'] is None)
        corrected, corrected_headers = request(MODERATION, review_path, moderator)
        request(MODERATION, review_path + '/commands', moderator, 'POST',
                {'action': 'APPROVE', 'versionId': claim['versionId'], 'versionNo': claim['versionNo']},
                {'If-Match': corrected_headers['ETag'], 'Idempotency-Key': RUN + '-stale-version'}, (409,))
        request(MODERATION, review_path + '/commands', moderator, 'POST',
                {'action': 'CLAIM', 'versionId': corrected['versionId'], 'versionNo': corrected['versionNo']},
                {'If-Match': corrected_headers['ETag'], 'Idempotency-Key': RUN + '-reclaim'}, (202,))
        review, headers = request(MODERATION, review_path, moderator)
        approve = {'action': 'APPROVE', 'versionId': review['versionId'], 'versionNo': review['versionNo']}
        request(MODERATION, review_path + '/commands', moderator, 'POST', approve,
                {'If-Match': claim_headers['If-Match'], 'Idempotency-Key': RUN + '-stale'}, (412,))
        request(MODERATION, review_path + '/commands', moderator, 'POST', approve,
                {'If-Match': headers['ETag'], 'Idempotency-Key': RUN + '-approve'}, (202,))
        eventually('Submission APPROVED', lambda: request(SUBMISSION, submission_path, student)[0],
                   lambda s: s['status'] == 'APPROVED')
        eventually('confirmed moderation approval', lambda: request(MODERATION, review_path, moderator)[0],
                   lambda r: r['status'] == 'APPROVED' and r['pendingCommandId'] is None)
        print('PASS: real auth, private clean media, cross-owner denial, replay, stale ETag/version, correction and confirmed approval; no catalog publication.')
    finally:
        had_failure = sys.exc_info()[0] is not None
        failed_logout = False
        for token in tokens.values():
            try:
                request(AUTH, '/api/v1/auth/logout', token, 'POST', expected=(204,))
            except (AssertionError, OSError):
                failed_logout = True
        if failed_logout:
            if not had_failure:
                raise AssertionError('Could not log out all smoke sessions')
            print('Could not log out all smoke sessions', file=sys.stderr)


if __name__ == '__main__':
    main()
