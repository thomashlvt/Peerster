$(document).ready(function(){
    let nodeIdEl = $("#node-id");
    let msgsEl = $("#messages");
    let peersEl = $("#peers");

    let msgContentEl = $("#send-message-content");
    let msgButtonEl = $("#send-message-button");
    let peerContentEl = $("#add-peer-content");
    let peerButtonEl = $("#add-peer-button");

    let dsdvOriginsEl = $("#dsdv-origins");

    let privateOriginEl = $("#private-origin");
    let privateMsgsEl = $("#private-messages");

    let privateMsgContentEl = $("#send-private-message-content");
    let privateMsgButtonEl = $("#private-message-button");
    let privateMsgDiv = $("#send-private-message");

    let selectFileEl = $("#select-file");
    let shareFileButtonEl = $("#share-file-button");
    let fileHashEl = $("#file-hash");
    let fileOriginEl = $("#file-origin");
    let fileDownloadEl = $("#download-file-button");
    let fileNameEl = $("#download-file-name");

    let keywordsEl = $("#keywords");
    let searchEl = $("#search-file-button");

    let resultsEl = $("#results");
    let downloadResultFilenameEl = $("#download-result-filename");

    let confirmedRumorsEl = $("#confirmed-rumors");

    let advancingToRoundEl = $("#advancing-to-round");

    let currentOrigin = null;

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
                    knownPeers.add(data.peers[i]);
                    peersEl.append("<li>" + data.peers[i] + "</li>")
                }
            }
        }).always(function(){
            setTimeout(refreshKnownPeers, 100);
        });
    }

    let dsdvOrigins = new Set();
    function refreshDSDV() {
        $.getJSON("dsdv", function(data) {
            for (i = 0; i < data.origins.length; i++) {
                if (!dsdvOrigins.has(data.origins[i])) {
                    dsdvOrigins.add(data.origins[i]);
                    dsdvOriginsEl.append("<li><a href=\"javascript:void(0)\" class='select-origin' data-id='" + data.origins[i] + "'>" + data.origins[i] + "</a></li>");

                    $(".select-origin").click(function(){
                        if (currentOrigin == null) {
                            privateMsgDiv.show()
                        }
                        refreshCurrentOrigin($(this).data("id"));
                        refreshPrivateMessages();
                    });
                }
            }
        }).always(function(){
            setTimeout(refreshDSDV, 100);
        })
    }

    refreshMessages();
    refreshKnownPeers();
    refreshDSDV();

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

    function addPrivateMessage(){
        $.ajax({
            type: 'POST',
            url: '/private/' + currentOrigin,
            data: JSON.stringify ({text: privateMsgContentEl.val()}),
            contentType: "application/json",
            dataType: 'json'
        });
    }

    privateMsgButtonEl.click(addPrivateMessage);
    privateMsgContentEl.keyup(function (e) {
        if (e.keyCode == 13) {
            addPrivateMessage();
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
    });

    let privateMsgs = new Set();
    function refreshPrivateMessages() {
        if (currentOrigin == null) {
            return;
        }
        $.getJSON("private/" + currentOrigin, function(data) {
            for (i = 0; i < data.msgs.length; i++) {
                if (!privateMsgs.has(data.msgs[i])) {
                    privateMsgs.add(data.msgs[i]);
                    privateMsgsEl.append("<li>" + data.msgs[i] + "</li>")
                }
            }
        }).always(function(){
            if (currentOrigin != null) {
                setTimeout(refreshPrivateMessages, 100);
            }
        })
    }

    function refreshCurrentOrigin(privateOrigin) {
        currentOrigin = privateOrigin;
        privateMsgs = new Set();
        privateMsgsEl.text("");
        privateOriginEl.text("Messages from " + privateOrigin);
    }

    // get shareable files
    $.getJSON("files", function(data) {
        for (i = 0; i < data.files.length; i++) {
            selectFileEl.append("<option>" + data.files[i] + "</option>");
        }
    });

    shareFileButtonEl.click(function(){
        $.ajax({
            type: 'POST',
            url: '/files/share',
            data: JSON.stringify ({file: selectFileEl.val()}),
            contentType: "application/json",
            dataType: 'json'
        });
    });

    fileDownloadEl.click(function(){
        $.ajax({
            type: 'POST',
            url: '/files/download',
            data: JSON.stringify ({hash: fileHashEl.val(), origin: fileOriginEl.val(), filename: fileNameEl.val()}),
            contentType: "application/json",
            dataType: 'json'
        });
    });

    function search(){
        // Reset results
        results = new Map();
        resultsEl.text("");
        $.ajax({
            type: 'POST',
            url: '/files/search',
            data: JSON.stringify ({keywords: keywordsEl.val()}),
            contentType: "application/json",
            dataType: 'json'
        });
    }

    searchEl.click(search);
    keywordsEl.keyup(function(e){
        if (e.keyCode == 13) {
            search();
        }
    });

    let results = new Map();
    function refreshResults() {
        $.getJSON("/files/search", function(data) {
            for (i = 0; i < data.results.length; i++) {
                if (!results.has(data.results[i].name)) {
                    results.set(data.results[i].name, data.results[i].meta);
                    resultsEl.append("<li><a href=\"javascript:void(0)\" class='download-result' data-id='" + data.results[i].meta + "'>" + data.results[i].name + "</a></li>");

                    $(".download-result").click(function(){
                        if (downloadResultFilenameEl.val() == "") {
                            alert("Please enter a filename for downloading the result!")
                        }
                        $.ajax({
                            type: 'POST',
                            url: '/files/download',
                            data: JSON.stringify ({hash: $(this).data("id"), origin: "", filename: downloadResultFilenameEl.val()}),
                            contentType: "application/json",
                            dataType: 'json'
                        });
                    });
                }
            }
        }).always(function(){
            setTimeout(refreshResults, 100);
        })
    }

    refreshResults();

    let confirmedRumors = new Set();
    function refreshConfirmedRumors(){
        $.getJSON("confirmed-rumors", function(data) {
            for (i = 0; i < data.confirmedRumors.length; i++) {
                cr = data.confirmedRumors[i];
                if (!confirmedRumors.has(JSON.stringify(cr))) {
                    confirmedRumors.add(JSON.stringify(cr));
                    descr = "origin " + cr.origin + " ID " + cr.id + " file name " + cr.filename + " size "  + cr.size + " metahash " + cr.meta;
                    confirmedRumorsEl.append("<li>" + descr + "</li>")
                }
            }
        }).always(function(){
            setTimeout(refreshConfirmedRumors, 100);
        });
    }

    refreshConfirmedRumors();

    let rounds = new Set();
    function refreshRounds(){
        $.getJSON("advancing-to-round", function(data) {
            for (i = 0; i < data.rounds.length; i++) {
                r = data.rounds[i];
                if (!rounds.has(JSON.stringify(r))) {
                    rounds.add(JSON.stringify(r));
                    descr = "round " + r.roundNum + " BASED ON CONFIRMED MESSAGES " + r.basedOn;
                    advancingToRoundEl.append("<li>" + descr + "</li>")
                }
            }
        }).always(function(){
            setTimeout(refreshRounds, 100);
        });
    }

    refreshRounds();

});