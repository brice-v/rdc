# Redis Clone in Go


This is my attempt at a redis clone in go.  Right now all that can be done is a simple PING and PONG.
Eventually the RESP protocol will be supported and ideally a bunch of commands.

It is not my goal to completely redo everything that has been done in Redis as its had a lot of work over the years. Just enough to have some fun and see how far I can go.

Also im not going to vendor the dependencies (if I have them) until a later point



### TODO

- To handle clients we can essentially have a counter, not defer close them at the beginning and then "quit" will close them as well as decrement the counter (esentially rc [ref counting])
- [ ] Refactor processing of commands to do some of the generic things easily
    - Such as checking args are correct len
    - Type and wrong type errors
    - maybe type of return value?
- [ ] Need to check if raw byte dump will be stored properly (might need to use custom string type for everything [which would just be a byte slice with length])
- [ ] Replace the get_type to use the constants instead to return
    - This is likely faster if its just a pointer comparison but we can benchmark that later
- [ ] Add RedisClient object to server to hold on to them
    - [ ] Make it so connection doesnt have to be passed to reply functions (maybe?)
- [ ] Implement Pub/Sub and 1.0 commands
- [ ] Flesh out client more once commands are done
- [ ] Implement proper testing if possible for both the client and server
- [ ] Client tests should mock out server replies but verify that it is sending properly
- [ ] See how fast we can make this (is there any beneficial way to use concurrency?)
    - Measure and Profile first
- [ ] Make some tools to visualize data and # of Req/s or Res/s or any other important metrics

### DONE

- [x] Need to keep first byte on string when we store it to get types for 'type' command (need to rework some stuff)
    - This is solved with the current architecture
- [x] use early returns in executeCommand, could just return bool and then return the replyXXX
- [x] Figure out how things will be stored internally
- [x] Have some validation layer on commands sent from client
    - This should be implemented server side
    - May need to normalize commands, ie. making them upper case to validate
        - But only with regards to commands
    - So pretty much the first bulk string/simple string or whatever should be uppercased
- [x] Implement RESP protocol
    - This should most likely be its own package
    - I know go will likely complain about not being able to find it however if its in the root directory
- [x] Dont use `\n` as the delimeter
    - [x] Use the proper format with `\r\n` appropriately
- [x] Make the client Read buffer based off of the first couple bytes
    - If its long enough it will tell us the byte amount
    - If its a simple string or something like that we can use a default such as 512
- [x] Server tests should respond to commands in RESP protocol and reply with the correct response