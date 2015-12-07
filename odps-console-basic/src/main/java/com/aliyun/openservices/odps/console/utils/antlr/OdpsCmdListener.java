// Generated from OdpsCmd.g4 by ANTLR 4.3
package com.aliyun.openservices.odps.console.utils.antlr;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link OdpsCmdParser}.
 */
public interface OdpsCmdListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#newline}.
	 * @param ctx the parse tree
	 */
	void enterNewline(@NotNull OdpsCmdParser.NewlineContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#newline}.
	 * @param ctx the parse tree
	 */
	void exitNewline(@NotNull OdpsCmdParser.NewlineContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#string}.
	 * @param ctx the parse tree
	 */
	void enterString(@NotNull OdpsCmdParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#string}.
	 * @param ctx the parse tree
	 */
	void exitString(@NotNull OdpsCmdParser.StringContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#cmds}.
	 * @param ctx the parse tree
	 */
	void enterCmds(@NotNull OdpsCmdParser.CmdsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#cmds}.
	 * @param ctx the parse tree
	 */
	void exitCmds(@NotNull OdpsCmdParser.CmdsContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#leftparen}.
	 * @param ctx the parse tree
	 */
	void enterLeftparen(@NotNull OdpsCmdParser.LeftparenContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#leftparen}.
	 * @param ctx the parse tree
	 */
	void exitLeftparen(@NotNull OdpsCmdParser.LeftparenContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#comment}.
	 * @param ctx the parse tree
	 */
	void enterComment(@NotNull OdpsCmdParser.CommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#comment}.
	 * @param ctx the parse tree
	 */
	void exitComment(@NotNull OdpsCmdParser.CommentContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#cmd}.
	 * @param ctx the parse tree
	 */
	void enterCmd(@NotNull OdpsCmdParser.CmdContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#cmd}.
	 * @param ctx the parse tree
	 */
	void exitCmd(@NotNull OdpsCmdParser.CmdContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#id}.
	 * @param ctx the parse tree
	 */
	void enterId(@NotNull OdpsCmdParser.IdContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#id}.
	 * @param ctx the parse tree
	 */
	void exitId(@NotNull OdpsCmdParser.IdContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#semicolon}.
	 * @param ctx the parse tree
	 */
	void enterSemicolon(@NotNull OdpsCmdParser.SemicolonContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#semicolon}.
	 * @param ctx the parse tree
	 */
	void exitSemicolon(@NotNull OdpsCmdParser.SemicolonContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#space}.
	 * @param ctx the parse tree
	 */
	void enterSpace(@NotNull OdpsCmdParser.SpaceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#space}.
	 * @param ctx the parse tree
	 */
	void exitSpace(@NotNull OdpsCmdParser.SpaceContext ctx);

	/**
	 * Enter a parse tree produced by {@link OdpsCmdParser#rightparen}.
	 * @param ctx the parse tree
	 */
	void enterRightparen(@NotNull OdpsCmdParser.RightparenContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsCmdParser#rightparen}.
	 * @param ctx the parse tree
	 */
	void exitRightparen(@NotNull OdpsCmdParser.RightparenContext ctx);
}