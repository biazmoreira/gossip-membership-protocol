/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
    this->completed = false;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    populateMembershipTable(id, port, memberNode->heartbeat, 1);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	// Reads message received and allocates it to message variable
    MessageHdr* message = (MessageHdr*) malloc(size * sizeof(char));
    memcpy(message, data, sizeof(MessageHdr));

    int id;
    short port;
    long heartbeat;
    memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
    memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
    memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));

    switch (message->msgType)
    {
    case JOINREQ:
        populateMembershipTable(id, port, heartbeat, memberNode->timeOutCounter);
        replyJOINREQ(id, port, heartbeat, memberNode->timeOutCounter);
        break;
    case JOINREP:
        memberNode->inGroup = true;
        readJOINREP(data);
        break;
    case GOSSIP:
       readGOSSIP(data);  
       break;
    default:
        break;
    }

    free(message); 
    return true;
}

/**
 * FUNCTION NAME: populateMembershipTable
 *
 * DESCRIPTION: Checks if node already exists before adding it as a new entry to the membership list
 * 				Apply transformations to turn a message into a newly created entry and insert it at the end of the vector
 */
void MP1Node::populateMembershipTable(int id, short port, long heartbeat, long timestamp) {
    
    Address addedNodedAddr;
    memset(&addedNodedAddr, 0, sizeof(Address));
    *(int *)(&addedNodedAddr.addr) = id;
    *(short *)(&addedNodedAddr.addr[4]) = port;
    
    bool exists = false;
    for (std::vector<MemberListEntry>::iterator i = memberNode->memberList.begin(); i != memberNode->memberList.end(); i++) {
        if (i->id == id) {
            exists = true;
        }
    }

    if(!exists) {
        MemberListEntry* e = new MemberListEntry(id, port, heartbeat, timestamp);
        memberNode->memberList.insert(memberNode->memberList.end(), *e);
        
        #ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &addedNodedAddr);
        #endif

        delete e;
    }

    if (memberNode->memberList.size() == par->EN_GPSZ) {
        completed = true;
    } 
}

/**
 * FUNCTION NAME: replyJOINREQ
 *
 * DESCRIPTION: Replies a JOINREQ message with a JOINREP after populating membership table
 */
void MP1Node::replyJOINREQ(int id, short port, long heartbeat, long timestamp) {
    // Startup JOINREP message
    size_t entrySize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    size_t listSize = memberNode->memberList.size();
    size_t messageSize = listSize * entrySize + sizeof(MessageHdr) + sizeof(int);

    MessageHdr* newMessage = (MessageHdr*) malloc(messageSize * sizeof(char));
    newMessage->msgType = JOINREP;
    
    // Transform message data into address
    Address destionationaddr;
    memset(&destionationaddr, 0, sizeof(Address));
    *(int *)(&destionationaddr.addr) = id;
    *(short *)(&destionationaddr.addr[4]) = port;

    // Transform membership list into message
    int numberOfItems = memberNode->memberList.size();
    memcpy((char *)(newMessage + 1), &numberOfItems, sizeof(int));
    int offset = sizeof(int);

    for(std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {     
        memcpy((char *)(newMessage + 1) + offset, &it->id, sizeof(int));
        offset += sizeof(int);
            
        memcpy((char *)(newMessage + 1) + offset, &it->port, sizeof(short));
        offset += sizeof(short);
            
        memcpy((char *)(newMessage + 1) + offset, &it->heartbeat, sizeof(long));
        offset += sizeof(long);
            
        memcpy((char *)(newMessage + 1) + offset, &it->timestamp, sizeof(long));
        offset += sizeof(long);
    }

    // Send message
    emulNet->ENsend(&memberNode->addr, &destionationaddr, (char*)newMessage, messageSize);

    free(newMessage);
}

/**
 * FUNCTION NAME: readJOINREP
 *
 * DESCRIPTION: Reads messages from join reply
 */
void MP1Node::readJOINREP(char* data) { 
    int size;
    memcpy(&size, data + sizeof(MessageHdr), sizeof(int));

    int offset = sizeof(int);
        
    for(int i = 0; i < size; i++) {
        int id;
        short port;
        long heartbeat;
        long timestamp;

        //ID    
        memcpy(&id, data + sizeof(MessageHdr) + offset, sizeof(int));
        offset += sizeof(int);
        
        //Port
        memcpy(&port, data + sizeof(MessageHdr) + offset, sizeof(short));
        offset += sizeof(short);

        //Heartbeat    
        memcpy(&heartbeat, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        //Heartbeat    
        memcpy(&timestamp, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        populateMembershipTable(id, port, heartbeat, timestamp);
    }
}

/**
 * FUNCTION NAME: readGOSSIP
 *
 * DESCRIPTION: Reads messages from gossip
 */
void MP1Node::readGOSSIP(char* data) { 
    int size;
    memcpy(&size, data + sizeof(MessageHdr), sizeof(int));

    int offset = sizeof(int);
        
    for(int i = 0; i < size; i++) {
        int id;
        short port;
        long heartbeat;
        long timestamp;

        //ID    
        memcpy(&id, data + sizeof(MessageHdr) + offset, sizeof(int));
        offset += sizeof(int);
        
        //Port
        memcpy(&port, data + sizeof(MessageHdr) + offset, sizeof(short));
        offset += sizeof(short);

        //Heartbeat    
        memcpy(&heartbeat, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        //Heartbeat    
        memcpy(&timestamp, data + sizeof(MessageHdr) + offset, sizeof(long));
        offset += sizeof(long);

        updateMembershipTable(id, port, heartbeat, timestamp);
    }
}

/**
 * FUNCTION NAME: updateMembershipTable
 *
 * DESCRIPTION: Checks if node already exists before adding it as a new entry to the membership list
 * 				Apply transformations to turn a message into a newly created entry and insert it at the end of the vector
 */
void MP1Node::updateMembershipTable(int id, short port, long heartbeat, long timestamp) {
    
    Address addedNodedAddr;
    memset(&addedNodedAddr, 0, sizeof(Address));
    *(int *)(&addedNodedAddr.addr) = id;
    *(short *)(&addedNodedAddr.addr[4]) = port;
    
    bool exists = false;
    for (std::vector<MemberListEntry>::iterator i = memberNode->memberList.begin(); i != memberNode->memberList.end(); i++) {
        if (i->id == id) {
            if (addedNodedAddr == memberNode->addr) {
                i->setheartbeat(memberNode->heartbeat);
                i->settimestamp(memberNode->timeOutCounter);
            } else {
                if (i->heartbeat < heartbeat) {
                    i->setheartbeat(heartbeat);
                    i->settimestamp(memberNode->timeOutCounter);
                }

                exists = true;
            }
        }
    }

    if (!exists && !completed) {
        populateMembershipTable(id, port, heartbeat, timestamp);
    } 
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    size_t entrySize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    size_t listSize = memberNode->memberList.size();
    size_t messageSize = listSize * entrySize + sizeof(MessageHdr) + sizeof(int);

    MessageHdr* newMessage = (MessageHdr*) malloc(messageSize * sizeof(char));
    newMessage->msgType = GOSSIP;
    
    int random = (rand() % listSize);

    // Transform message data into address
    Address destionationaddr;
    memset(&destionationaddr, 0, sizeof(Address));
    *(int *)(&destionationaddr.addr) = memberNode->memberList.at(random).id;
    *(short *)(&destionationaddr.addr[4]) = memberNode->memberList.at(random).port;

    // Transform membership list into message
    int numberOfItems = memberNode->memberList.size();
    memcpy((char *)(newMessage + 1), &numberOfItems, sizeof(int));
    int offset = sizeof(int);

    memberNode->heartbeat++;
    memberNode->timeOutCounter++;

    for(std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {     
        memcpy((char *)(newMessage + 1) + offset, &it->id, sizeof(int));
        offset += sizeof(int);
            
        memcpy((char *)(newMessage + 1) + offset, &it->port, sizeof(short));
        offset += sizeof(short);
            
        memcpy((char *)(newMessage + 1) + offset, &it->heartbeat, sizeof(long));
        offset += sizeof(long);
            
        memcpy((char *)(newMessage + 1) + offset, &it->timestamp, sizeof(long));
        offset += sizeof(long);
    }

    // Send message
    emulNet->ENsend(&memberNode->addr, &destionationaddr, (char*)newMessage, messageSize);

    free(newMessage);

    //====
    
    for (int i = 0; i < memberNode->memberList.size(); i++) {
        MemberListEntry m = memberNode->memberList.at(i);
        Address destionationaddr;
        memset(&destionationaddr, 0, sizeof(Address));
        *(int *)(&destionationaddr.addr) = m.getid();
        *(short *)(&destionationaddr.addr[4]) = m.getport();

        if (destionationaddr.getAddress() != memberNode->addr.getAddress()) {
            if(memberNode->timeOutCounter - m.timestamp > TREMOVE) {
                memberNode->memberList.erase(memberNode->memberList.begin() + i);
                #ifdef DEBUGLOG
                log->logNodeRemove(&memberNode->addr, &destionationaddr);
                #endif
            }
        }
    }
        
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}