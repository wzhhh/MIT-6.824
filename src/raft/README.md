some notice

cc: https://blog.csdn.net/ekxavol/article/details/118426162?utm_medium=distribute.pc_relevant.none-task-blog-2~default~baidujs_title~default-1.no_search_link&spm=1001.2101.3001.4242

Lab 2A
A1.简介
根据Figure 2的思路，实现RequestVoteRPC以及其Handler，实现Raft的选举机制。

需要注意的点主要包括：
1）必须严格遵循Figure 2的逻辑
2）VotedFor和CurrentTerm这两个变量是一组变量
3）我的做法是通过维护一个时间变量lastHeartbeatEverSeen来实现超时机制，但可能可以通过go的Timer来实现
4）reset timer的时机有必要进行一番考虑。总体而言，当一个raft接收到某条信息，该认为有必要乐观地假设自己是follower状态的时候，它就可以reset timer

A2.一些可能的问题/踩坑情况
基本都是基于实现不佳所导致的！…

A00
问题：不断进行选举。
分析：debug后发现接收到自己的心跳的时候把自己设置为follower
结论：接收到heartbeat之后判断我自己是否为heartbeat发送者，如果是，不修改为follower
A01
问题：不断进行选举
分析：debug后发现某一个follower掉线之后，自己不断增长term，回来之后term远远在其他raft之上，但由于其log远远落后，导致不断分票。
结论：根据Figure 2对currentTerm的描述(last term ever seen)，在每个RPC中都判断并更新自身的term
A02
问题：不断进行选举
分析：存在细节问题，对currentTerm和votedFor没有进行联动修改，导致更新currentTerm的时候，votedFor并未更新，从而永远投不出票。
结论：currentTerm和votedFor必须进行联动修改，凡是currentTerm有提升，就必须让自己的votedFor=-1

A03
问题：选举出多个leader
分析：投票给他人之后自身的状态没有修改，选举超时，拉票时自己默认有自己一票，导致本轮自己多投出了一票
结论：给他人投票后，立刻重置自己的reset timer

A04
问题：如果有一个单向网络，一台掉线的follower发现自己超时，改为candidate，不断发送RequestVote，而其他主机的信息传达不到它的身上，岂不是会造成一个永远达不成共识的网络？（就算短暂地达成共识，其他Raft也会在一个electionTimeout后，看到该raft发来的新term，并更新自身为follower）
结论：根据公开课第6节提到的内容，本问题没必要考虑。如果真的有这种病态的网络，那么可以把rpc改成双向传输才奏效。

Lab 2B
B1.简介
这个lab相对比较复杂，也是Raft 共识机制实现的主要部分。跟随Raft论文以及Guide的指引，我的大体设计思路如下：

1)听到start()的时候作为leader立即追加本地日志并立刻返回
2)CheckAndAppendEntries()方法定时检查是否需要向follower追加日志
3)TriggerApplyMsg()方法定时检查，根据commitIndex和lastApplied的区别，决定是否需要向applyCh（server）发送信息

B2.可能需要注意的要点
4)matchIndex是一个悲观预测，nextIndex是最乐观的预测，有可能追加失败，此时应该设计函数的递归调用。同时需要注意matchIndex永远递增；nextIndex有可能递减；nextIndex递减的时候可能会涉及发送RPC，此时必须释放锁防止阻塞。所以，可能会导致并发问题。例如：某使得nextIndex减到100的false reply（RPC1），返回到一半网络阻塞了，在这个RPC的阻塞过程中，Leader成功地利用别的RPC把该follower的log更新到最前面了，此时RPC1终于到达。此时应该拒绝RPC1，方法是利用nextIndex>matchIndex这个机制来进行判断
5)commitIndex用来和lastApplied作为对比，作用是和Server进行交互。这两个变量和nextIndex，matchIndex（leader和follower之间的关系）之间有较为显著的区别
6)在心跳中附带commit信息。否则，只发送一条指令时，follower无法知道该log已经commit
7)笔者的做法是，凡是带rpc的都把锁解开，不过这个设计可能可以改进。
8)在2）的过程中需要注意不能够直接追加过期的log（等到至少一条当期的log出现后才开始追加），在论文中这点有强调。
9)2B对2A进行了更为严格的检测，out of date在论文中有严格定义，需遵循。该点确保了leader是一个正确的leader（拥有了所有的commited log）。2A良好的超时机制设计有利于在不断掉线的情况下也能尽量快地选出合法leader
10)follower需要严格遵循Figure 2中的话，删除并且只删除：冲突节点以及该节点后面的所有节点

Lab 2C
相比Lab 2B，2C比较简单，主要是对Raft做了一些持久化和优化处理。

1）持久化到persister里的3个state，在实质上保存了leader选举信息以及log信息，其他信息都可以从这三个信息中生成。
2）快速回退优化可以根据教授提到的XLength机制或者根据guide里提到的机制实现。
3）Figure 8 unreliable相对来说比较容易出问题。但是只要把握住leader掌握所有commit index这一核心，（或者说在该分布式系统中，overlap机制导致了commit的majority实际上成为了某种主体，leader只是一个代表而已），配合不断地debug，就能够通过该测试。
4）在该实验的设计中，Call()最终一定会返回，返回false代表超时，此时返回的reply不携带任何有效信息。
————————————————
版权声明：本文为CSDN博主「ekxavol」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/ekxavol/article/details/118426162
