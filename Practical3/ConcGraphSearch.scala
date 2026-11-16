import ox.scl._

/*
Design decisions:
Stack:
I considered a monitor for the whole thing to ensure a perfectly depth first search.
However, a perfect search is not required for this problem - approximating 
similar to the style in the lectures is sufficient.
I set up a server to avoid race conditions. 
I used a threadsafe count to account for the non-existent solution case. 

Remainder:
I used a bag of tasks approach with the bag being the concurrent stack to fit the DFS requirement. 
I run the workers with another thread to add the start.
*/


class ConcurrentStack[T](n: Int) {  // n is the # of workers
	private val stack = new scala.collection.mutable.Stack[T]()
	private val pushChan = new SyncChan[T]()
	private val popChan = new SyncChan[T]()
	private val shutdownChan = new SyncChan[Unit]()
	private var nWaiting = 0  // Threadsafe count of number of workers waiting for a popped value

	def push(x:T): Unit = try {pushChan!x} catch{case _:Closed => {}}  // Sends to server. 
	// May be closed since another worker shutdown the stack - just does nothing in that situation. 
	def pop: Option[T] = {
		try {
		synchronized {nWaiting += 1}
		val temp = popChan?()
		synchronized {nWaiting -= 1}
		Some(temp)
		} catch {case _:Closed => None}  // Again, if another worker shut the stack, returns None to close that worker. 
	}
	private def server = thread {
		serve (
			pushChan =?=> {x=>stack.push(x)}  // Adds to stack
			| !stack.isEmpty && popChan =!=> {stack.pop()}  // Takes from stack
			| stack.isEmpty && nWaiting == n && shutdownChan =!=> {}  // Shuts down if all waiting and nothing in the stack
		)
	}
	def shutdown = {  // External shutdown control
		try {shutdownChan!()} catch{case _:Closed => {}}  // The case of multiple solutions
	}
	private def shutdownChanHandler = thread {  // Shuts down
		shutdownChan?()
		pushChan.close
		popChan.close
		shutdownChan.close
	}
	// Runs server and shutdown utility. 
	server.fork
	shutdownChanHandler.fork
}

class ConcGraphSearch[N](g: Graph[N]) extends GraphSearch[N](g){
  	/**The number of workers. */
  	val numWorkers = 8
  	var result: Option[N] = None
	/** Perform a depth-first search in g, starting from start, for a node that
    	* satisfies isTarget. */
  	def apply(start: N, isTarget: N => Boolean): Option[N] = {
		result = None
		val taskStack = new ConcurrentStack[N](numWorkers)
		run(thread {taskStack.push(start)} || || (for (i <- 0 until numWorkers) yield worker(taskStack, isTarget)))
		result
  	}
	
	def worker(stack: ConcurrentStack[N], isTarget: N => Boolean) = thread {
		var continue = true
		while (continue) {  // Substitute for the empty condition which would not be threadsafe
			val popped = stack.pop
			popped match {
				case Some(n) => {  // popchan still active - no soln found and items remain in the stack
					for(n1 <- g.succs(n)){  // Goes through connections in the graph
						if(isTarget(n1)) {
							synchronized{
								result = Some(n1) // Writes answer
								stack.shutdown 
								continue = false
							}
						}
						else {
							stack.push(n1)
						}
					}
				}
				case None => continue = false  // popchan closed - answer found or nothing in stack for anyone. 
			}
		}
	}
}