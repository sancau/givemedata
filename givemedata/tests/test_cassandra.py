from ..givemedata import parse_cassandra_cs


def test_parse_cassandra_cs():
    cs = 'cassandra://user:password@10.05.02.22--10.05.02.23/keyspace'
    res = parse_cassandra_cs(cs=cs)

    assert res['hosts'] == ['10.05.02.22', '10.05.02.23']
    assert res['username'] == 'user'
    assert res['password'] == 'password'
    assert res['keyspace'] == 'keyspace'
