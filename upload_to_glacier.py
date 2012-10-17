#!/usr/bin/python

import boto.glacier
import boto.glacier.layer2
import datetime
import sys

# To include 'aws_consts', which should not be part of the project.
sys.path.append('../')
import aws_consts

layer2 = boto.glacier.layer2.Layer2(
    aws_access_key_id = aws_consts.ACCESS_KEY_ID,
    aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY,
    region_name=aws_consts.REGION_NAME)

def get_name_candidate(vault_basename, seq_num):
    return '%s_%s' % (vault_basename, seq_num)


def create_vault(vault_basename):
    # TODO: support vaults more than 1000
    existing_vault_names = set([])
    for existing_vault in layer2.list_vaults():
        existing_vault_names.add(existing_vault.name)
        pass

    seq_num = 1
    vault_name = get_name_candidate(vault_basename, seq_num)
    while vault_name in existing_vault_names:
        seq_num += 1
        vault_name = get_name_candidate(vault_basename, seq_num)
        pass
    return layer2.create_vault(vault_name)

if __name__ == '__main__':
    sys.exit(0)
    if len(sys.argv) < 2:
        print >>sys.stderr, 'usage: %s file_name' % sys.argv[0]
        sys.exit(1)
        pass

    filename = sys.argv[1]
    today = datetime.date.today()
    vault_basename = 'Backup_%04d%02d%02d' % (today.year,
                                              today.month,
                                              today.day)
    vault = create_vault(vault_basename)
    assert vault
    archive_id = vault.create_archive_from_file(filename=filename)
    print '%s\n%s' % (vault.name, archiv_id)
    pass

