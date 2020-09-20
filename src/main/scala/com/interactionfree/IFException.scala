package com.interactionfree

class IFException(message: String = "", cause: Throwable = null) extends Exception(message, cause) {}