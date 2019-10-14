$(document).ready(function(){
    let nodeIdEl = $("#node-id");
    let msgsEl = $("#messages");
    let peersEl = $("#peers");

    let msgContentEl = $("#send-message-content");
    let msgButtonEl = $("#send-message-button");
    let peerContentEl = $("#add-peer-content");
    let peerButtonEl = $("#add-peer-button");

    let knownMessages = new Set();
    function refreshMessages(){
        $.getJSON("message", function(data) {
            for (i = 0; i < data.msgs.length; i++) {
                m = data.msgs[i];
                if (!knownMessages.has(JSON.stringify(m))) {
                    knownMessages.add(JSON.stringify(m));
                    descr = "FROM " + m.origin + " (ID " + m.id + "): " + m.text;
                    msgsEl.append("<li>" + descr + "</li>")
                }
            }
        }).always(function(){
            setTimeout(refreshMessages, 100);
        });
    }

    let knownPeers = new Set();
    function refreshKnownPeers(){
        $.getJSON("node", function(data) {
            for (i = 0; i < data.peers.length; i++) {
                if (!knownPeers.has(data.peers[i])) {
                    knownPeers.add(data.peers[i])
                    peersEl.append("<li>" + data.peers[i] + "</li>")
                }
            }
        }).always(function(){
            setTimeout(refreshKnownPeers, 100);
        });
    }

    refreshMessages();
    refreshKnownPeers();

    $.getJSON("id", function(data) {
        nodeIdEl.html("<p>" + data.id + "</p>");
    });

    function addMessage(){
        $.ajax({
            type: 'POST',
            url: '/message',
            data: JSON.stringify ({text: msgContentEl.val()}),
            contentType: "application/json",
            dataType: 'json'
        });
    }

    msgButtonEl.click(addMessage);
    msgContentEl.keyup(function (e) {
        if (e.keyCode == 13) {
            addMessage();
        }
    });

    function addPeer(){
        ip = peerContentEl.val();
        if (!/^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]+$/.test(ip)) {
            alert("Please enter a valid peer");
            return;
        }
        $.ajax({
            type: 'POST',
            url: '/node',
            data: JSON.stringify ({peer: ip}),
            contentType: "application/json",
            dataType: 'json'
        });
    }

    peerButtonEl.click(addPeer);
    peerContentEl.keyup(function(e){
        if (e.keyCode == 13) {
            addPeer();
        }
    })
});