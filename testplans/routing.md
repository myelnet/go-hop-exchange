# Content Routing

This plan evaluates different strategies for content drouting.
For more information on how to run it please follow instructions on the testplans [readme](/testplans).

The main solution is currently based on gossip messaging though we may add other
solutions in the future for different use cases.

## Global parameters

### Groups
In this plan we work with different node groups to replicate real world scenarios we expect to encounter
in the Myel network. These groups are defined in regards to the session we run in our tesplan.
- Nodes in the `clients` group make queries for content during the lifecycle of our session.
- Nodes in the `providers` group store the content requested by clients and reply to client queries.
- Nodes in the `bystanders` group may be client or provider nodes though aren't actively querying for content
or storing the content clients in our session are looking for. Since all nodes in the network won't be storing
or requesting the same content we assume a majority of node are bystanders to a given session.

### Connections
The way peers are connected in the network heavily influences the performance of discovery sessions.
To simulate real world network topologies we use a combination of bootstrapping by connecting some nodes
directly and letting the DHT randomly distribute connections.
- `bootstrap` defines the number of nodes we directly connect with when setting up our node. A higher number
of bootstrap nodes means our network will be more densely connected in a shorter time frame.
- `min_conns` is the minimum of connection our node routing should strive to reach with the DHT.
- `max_conns` is the amount after which our node will start pruning connections.
Hence we expect the number of nodes connected to reach some number between min and max conns. 

### Traffic Shaping
We assume peers will have different network conditions and randomly generate parameters between a minimum and maximum.
- `latency` is generated between a given min and max. We use values expected to be encountered in a relatively close geographic area i.e. Europe as our use case optimizes for localized usage.
- `bandwidth` is generated based on average bandwidth for our target user segment, end users of recent and mid to high performance devices. We may try on lower latency connections to get and idea as well.
- `jitter` is generated based on typical values encountered in real world situations in which users maybe using more brittle wireless connections over Ethernet connected servers.

## Gossip

In these experiments we compare 2 different implementations, one (address broadcast) in which the publisher 
network address is sent along in the message and the recipient streams the response back directly and the other 
(recursive forwarding) in which each peer in the gossip transmission chain forwards back to the previous sender.

### [Low Content Replication](/testplans/_compositions/low_content_replication.toml)

This composition evaluates the performance of the network with low to no content replication over a
large network.

#### Results

Ubuntu AMD Ryzen 9 3900XT 12-Core Processor - 64GiB DDR4

Typical time to first offer: mean ± standard deviations (24 samples)

| Solution        | 10 Instances (ms) | 20 Instances (ms)  | 30 Instances (ms) | 40 Instances (ms) |
| :--- | ---: | ---: | ---: | ---: |
| `gossip - AB`   |     `825` `±104`  |     `1337` `±1071` |    `2111` `±1376` |    `2081` `±1275` | 
| `gossip - RF`   |    `1014` `±344`  |     `1187` `±447`  |    `1186` `±442`  |    `1445` `±457`  |


#### Interpretation

As the size of the network grows, the client becomes less likely to be directly connected to ther provider
of the content it's looking for. Dialing the provider incurs some overhead which can reduce
the speed of query by 2-3X. As a result, the standard devation shows the very large difference between queries
in which client and provider are directly connected vs when they're not.
When forwarding back to the previous sender we reuse existing connections so the speed depends on the number of
hops between peers and makes the speed more consistant.

### [High Content Replication](/testplans/_compositions/high_content_replication.toml)

This composition evaluates the performance of the network with high content replication over a large 
network.

#### Results

Ubuntu AMD Ryzen 9 3900XT 12-Core Processor - 64GiB DDR4

Typical time to first offer: mean ± standard deviations (24 samples)

| Solution         | 10 Instances (ms) | 20 Instances (ms)  | 30 Instances (ms) | 40 Instances (ms) |
| :--- | ---: | ---: | ---: | ---: |
| `gossip - AB`    |       `697` `±25` |     	`720` `±40` |      `844` `±499` |      `776` `±101` | 
| `gossip - RF`    |       `686` `±24` | 	`714` `±25` |      `817` `±234` |      `906` `±333` | 

#### Interpretation

A higher content replication means a higher likelihood that the client is directly connected with a 
provider hence a majority of queries are very quick to execute even with a high latency and jitter.
Using recursive forwarding yields very similar speed.

### [Network Segmentation By Region](/testplans/_compositions/network_segment_region.toml)

This composition demonstrates the segmentation of the gossip network into different topics based on geographic
regions. Messages are only published to a topic in which the subscribers have relatively similar latency.

#### Results

Ubuntu AMD Ryzen 9 3900XT 12-Core Processor - 64GiB DDR4

Typical time to first offer: mean ± standard deviations (24 samples)

| Solution         | 10 Instances (ms) | 20 Instances (ms)  | 30 Instances (ms) | 40 Instances (ms) |
| :--- | ---: | ---: | ---: | ---: |
| `gossip - AB`    |        `96` `±16` |       `132` `±129` |       `125` `±31` |      `240` `±252` | 
| `gossip - RF`    | 	   `148` `±87` |       `143` `±51`  |       `136` `±50` |      `168` `±78`  |

#### Interpretation

Since the message is only published to peers at the lowest latency in the network, propagation is extremely
fast. Even if peers are likely to dial each other, when they are very close to each other this extra step
doesn't impact the speed as significantly as with peers with higher latency.
RF looks very similar in speed though significantly more consistant across samples.

### [Network Segmentation By Content](/testplans/_compositions/network_segment_content.toml)

This composition demonstrates the segmentation of the gossip network by type of content. I.e. an application
has created its own subnetwork in which clients can query the topic to find their content. Currently the 
discovery session can only publish to a region topic so we use region topics to test but we include peers with 
different latency as opposed to the previous test.

#### Results

Ubuntu AMD Ryzen 9 3900XT 12-Core Processor - 64GiB DDR4

Typical time to first offer: mean ± standard deviations (24 samples)

| Solution         | 10 Instances (ms) | 20 Instances (ms) | 30 Instances (ms) | 40 Instances (ms) |
| :--- | ---: | ---: | ---: | ---: |
| `gossip - AB`    |       `707` `±29` |      `713` `±36`  |     `1076` `±881` |    `1552` `±1206` | 
| `gossip - RF`    |       `708` `±28` |      `752` `±146` |      `863` `±267` |     `846` `±258`  |

#### Interpretation

Segmenting the network into non-geographic topics does not seem to improve performance at this scale.
This is because peers with different latencies can subscribe to the same topic thus it does not guarrantee
peers will be nearby.
RF appears to offer better performance in these conditions, might be because smaller amount of subscribers
means less hops though it is very likely peers won't be directly connected.

### [Network Segmentation By Cluster Difusion](/testplans/_compositions/network_segment_clusters.toml)
