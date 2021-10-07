defmodule RedixClustered.SlotsTest do
  use ExUnit.Case, async: true

  alias RedixClustered.Slots

  @name :slots_test
  @host System.get_env("REDIS_HOST", "127.0.0.1")
  @port System.get_env("REDIS_PORT", "6379")

  setup do
    opts = [name: @name, host: @host, port: @port]
    start_supervised!({RedixClustered, opts})
    :ok
  end

  test "gets the slot for redis keys" do
    assert Slots.hash("a") == 15495
    assert Slots.hash("b") == 3300
    assert Slots.hash("c") == 7365
    assert Slots.hash("d") == 11298
  end

  test "handles explicit hash keys" do
    assert Slots.hash("ab{c}d") == 7365
    assert Slots.hash("ab{c}d{e}f") == 7365
    assert Slots.hash("ab{c}d{{f") == 7365
    assert Slots.hash("ab{c}d}}f") == 7365
    assert Slots.hash("ab{c") == 4619
    assert Slots.hash("abc}") == 11054
  end

  test "handles atoms" do
    assert Slots.hash(:a) == 15495
    assert Slots.hash(:b) == 3300
    assert Slots.hash(:c) == 7365
    assert Slots.hash(:d) == 11298
  end

  test "parses cluster slots" do
    host1 = ["host1", 7001, "some-str-1"]
    host2 = ["host2", 7002, "some-str-2"]
    host3 = ["host3", 7003, "some-str-3"]
    host4 = ["host4", 7004, "some-str-4"]
    host5 = ["host5", 7005, "some-str-5"]
    host6 = ["host6", 7006, "some-str-6"]

    slot1 = [10, 20, host1, host2, host3]
    slot2 = [30, 40, host4]
    slot3 = [50, 60, host5, host6]
    slot4 = [99, "bad", "data"]

    result = Slots.parse_cluster_slots([slot1, slot2, slot3, slot4])

    assert length(result) == 3
    assert Enum.at(result, 0) == {"host1:7001", 10..20}
    assert Enum.at(result, 1) == {"host4:7004", 30..40}
    assert Enum.at(result, 2) == {"host5:7005", 50..60}
  end

  test "finds nodes for keys" do
    Slots.set_slots(@name, [
      {"redis:456", 5461..10922},
      {"redis:123", 0..5640},
      {"redis:789", 10923..16383}
    ])

    assert Slots.lookup(@name, "a") == "redis:789"
    assert Slots.lookup(@name, "b") == "redis:123"
    assert Slots.lookup(@name, "c") == "redis:456"
    assert Slots.lookup(@name, "d") == "redis:789"
  end

  test "handles missing slots" do
    assert Slots.slots(@name) == []
    assert Slots.lookup(@name, "c") == nil

    Slots.set_slots(@name, [
      {"redis:123", 0..5640},
      {"redis:789", 10923..16383}
    ])

    assert Slots.lookup(@name, "c") == nil
  end

  test "debounces refreshing slots" do
    Slots.set_slots(@name, [{"redis:123", 4..56}])
    last_time = Slots.last_refreshed(@name)

    Slots.refresh(@name, "fake-pid")
    Slots.refresh(@name, "fake-pid")
    Slots.refresh(@name, "fake-pid")

    assert Slots.last_refreshed(@name) == last_time
    assert Slots.slots(@name) == [{"redis:123", 4..56}]
  end

  describe "redis cluster" do
    @describetag :cluster

    test "refreshes cluster slots" do
      last_time = Slots.last_refreshed(@name)
      assert Slots.slots(@name) == []

      conn = RedixClustered.Registry.connect(@name)
      Slots.refresh(@name, conn)

      assert Slots.last_refreshed(@name) > last_time
      assert length(Slots.slots(@name)) >= 1
    end
  end
end
