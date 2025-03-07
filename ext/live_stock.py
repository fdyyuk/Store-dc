import logging
import asyncio
from typing import Optional, Dict, List
from datetime import datetime

import discord
from discord.ext import commands, tasks
from .constants import (
    Status,          # Untuk status stok
    COLORS,         # Untuk warna embed
    UPDATE_INTERVAL,# Untuk interval update (55 seconds)
    MESSAGES,       # Untuk pesan error/status
    CACHE_TIMEOUT  # Untuk cache message ID
)

from database import get_connection
from .base_handler import BaseLockHandler
from .cache_manager import CacheManager
from .product_manager import ProductManagerService

class LiveStockManager(BaseLockHandler):
    _instance = None
    _instance_lock = asyncio.Lock()

    def __new__(cls, bot):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialized = False
        return cls._instance

    def __init__(self, bot):
        if not self.initialized:
            super().__init__()
            self.bot = bot
            self.logger = logging.getLogger("LiveStockManager")
            self.cache_manager = CacheManager()
            self.product_manager = ProductManagerService(bot)
            self.stock_channel_id = int(self.bot.config.get('id_live_stock', 0))
            self.current_stock_message: Optional[discord.Message] = None
            self.initialized = True

    async def create_stock_embed(self) -> discord.Embed:
        try:
            products = await self.product_manager.get_all_products()
            embed = discord.Embed(
                title="🌟 Live Stock Status",
                description=(
                    "```\n"
                    "Welcome to our Growtopia Shop!\n"
                    "Real-time stock information updated every minute\n"
                    "```"
                ),
                color=COLORS['info']
            )
            embed.add_field(
                name="🕒 Server Time",
                value=f"```yml\n{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC```",
                inline=False
            )
            for product in products:
                stock_count = await self.product_manager.get_stock_count(product['code'])
                status_emoji = "🟢" if stock_count > 0 else "🔴"
                status_text = "Available" if stock_count > 0 else "Out of Stock"
                field_value = (
                    "```yml\n"
                    f"Price: {product['price']:,} WL\n"
                    f"Stock: {stock_count} units\n"
                    f"Status: {status_text}\n"
                    "```"
                )
                embed.add_field(
                    name=f"{status_emoji} {product['name']} ({product['code']})",
                    value=field_value,
                    inline=True
                )
            embed.set_footer(
                text="Last Updated",
                icon_url=self.bot.user.display_avatar.url
            )
            embed.timestamp = datetime.utcnow()
            return embed
        except Exception as e:
            self.logger.error(f"Error creating stock embed: {e}")
            raise

    async def get_or_create_stock_message(self) -> Optional[discord.Message]:
        if not self.stock_channel_id:
            self.logger.error("Stock channel ID not configured!")
            return None
        channel = self.bot.get_channel(self.stock_channel_id)
        if not channel:
            self.logger.error(f"Could not find stock channel {self.stock_channel_id}")
            return None
        try:
            message_id = await self.cache_manager.get("live_stock_message_id")
            if message_id:
                try:
                    message = await channel.fetch_message(message_id)
                    self.current_stock_message = message
                    return message
                except discord.NotFound:
                    await self.cache_manager.delete("live_stock_message_id")
                except Exception as e:
                    self.logger.error(f"Error fetching stock message: {e}")
            embed = await self.create_stock_embed()
            message = await channel.send(embed=embed)
            self.current_stock_message = message
            await self.cache_manager.set(
                "live_stock_message_id",
                message.id,
                expires_in=CACHE_TIMEOUT,
                permanent=True
            )
            return message
        except Exception as e:
            self.logger.error(f"Error in get_or_create_stock_message: {e}")
            return None

    async def update_stock_display(self) -> bool:
        try:
            if not self.current_stock_message:
                self.current_stock_message = await self.get_or_create_stock_message()
            if not self.current_stock_message:
                return False
            embed = await self.create_stock_embed()
            await self.current_stock_message.edit(embed=embed)
            return True
        except Exception as e:
            self.logger.error(f"Error updating stock display: {e}")
            return False

    async def cleanup(self):
        try:
            if self.current_stock_message:
                await self.current_stock_message.edit(
                    content="Shop is currently offline. Please wait...",
                    embed=None
                )
        except Exception as e:
            self.logger.error(f"Error in cleanup: {e}")
            
class LiveStockCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.stock_manager = LiveStockManager(bot)
        self.logger = logging.getLogger("LiveStockCog")
        self.update_stock.start()

    @tasks.loop(seconds=UPDATE_INTERVAL)  # Menggunakan UPDATE_INTERVAL dari constants
    async def update_stock(self):
        """Update stock display periodically"""
        try:
            await self.stock_manager.update_stock_display()
        except Exception as e:
            self.logger.error(f"Error in stock update loop: {e}")

    @update_stock.before_loop
    async def before_update_stock(self):
        """Wait until bot is ready before starting the loop"""
        await self.bot.wait_until_ready()
        # Ubah format output agar tidak ada prefix f-string
        print('Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): ' + datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        print('Current User\'s Login: ' + str(self.bot.user))
    
    async def cog_unload(self):
        """Cleanup when unloading cog"""
        self.update_stock.cancel()
        await self.stock_manager.cleanup()
        self.logger.info("LiveStockCog unloaded")

async def setup(bot):
    if not hasattr(bot, 'live_stock_loaded'):
        await bot.add_cog(LiveStockCog(bot))
        bot.live_stock_loaded = True
        # Load live_buttons setelah stock loaded
        if 'ext.live_buttons' not in bot.extensions:
            await bot.load_extension('ext.live_buttons')